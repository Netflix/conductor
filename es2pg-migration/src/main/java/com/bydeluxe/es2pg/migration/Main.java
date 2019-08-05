package com.bydeluxe.es2pg.migration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class Main {
	private static String FORMAT = "H'h' m'm' s's'";
	private static Logger logger = LogManager.getLogger(Main.class);
	private AtomicBoolean keepPooling = new AtomicBoolean(true);
	private BlockingDeque<String> workflowQueue = new LinkedBlockingDeque<>();
	private Map<String, AtomicInteger> failedStats = new ConcurrentHashMap<>();
	private AppConfig config = AppConfig.getInstance();
	private CountDownLatch latch = new CountDownLatch(config.queueWorkers());
	private HikariDataSource dataSource;
	private RestClientBuilder builder;
	private Dao dao;
	private long esTotalWorkflows;

	public static void main(String[] args) {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		try {
			logger.info("ES 2 PG Migration started");

			Main main = new Main();
			main.start();
			System.exit(0);
		} catch (Throwable ex) {
			logger.error("Main failed with " + ex.getMessage(), ex);
			System.exit(-1);
		}
	}

	private void start() throws Exception {
		long start = System.currentTimeMillis();

		String clusterAddress = config.source();
		logger.info("Creating ES client");
		if (StringUtils.isEmpty(clusterAddress)) {
			throw new RuntimeException("No ElasticSearch Url defined. Exiting");
		}

		builder = RestClient.builder(HttpHost.create(config.source()));
		builder.setMaxRetryTimeoutMillis(60_000)
			.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
				.setConnectionRequestTimeout(0)
				.setSocketTimeout(60_000)
				.setConnectTimeout(60_000));

		String url = String.format("jdbc:postgresql://%s:%s/%s", config.auroraHost(), config.auroraPort(), config.auroraDb());

		HikariConfig poolConfig = new HikariConfig();
		poolConfig.setJdbcUrl(url);
		poolConfig.setUsername(config.auroraUser());
		poolConfig.setPassword(config.auroraPassword());
		poolConfig.setAutoCommit(false);
		poolConfig.setConnectionTimeout(60_000);
		poolConfig.setMinimumIdle(config.queueWorkers());
		poolConfig.setMaximumPoolSize(config.queueWorkers() * 2);
		poolConfig.addDataSourceProperty("cachePrepStmts", "true");
		poolConfig.addDataSourceProperty("prepStmtCacheSize", "250");
		poolConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

		logger.info("Creating PG client");
		dataSource = new HikariDataSource(poolConfig);

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

		dao = new Dao(dataSource, mapper);

		logger.info("Grabbing definitions ...");
		grabMetadata();

		logger.info("Starting workers ...");
		startWorkers();

		logger.info("Grabbing workflows ...");
		grabWorkflows();

		logger.info("Waiting for workers to complete");
		waitWorkflows();

		logger.info("Post migration data cleanup...");
		dataCleanup();

		logger.info("Grabbing queues ...");
		grabQueues();

		logger.info("Requeue async ...");
		requeueAsync();

		logger.info("Requeue decider ...");
		requeueDecider();

		String duration = DurationFormatUtils.formatDuration(System.currentTimeMillis() - start, FORMAT, true);
		logger.info("ES2PG migration done, took " + duration);
	}

	private void startWorkers() {
		Runnable runnable = () -> {

			while (keepPooling.get() || !workflowQueue.isEmpty()) {
				String workflowId = workflowQueue.poll();
				if (workflowId != null) {
					try {
						try (Connection tx = dataSource.getConnection()) {
							tx.setAutoCommit(false);
							try {

								processWorkflow(workflowId, tx);
								tx.commit();

							} catch (Throwable th) {
								tx.rollback();
								throw th;
							}
						}
					} catch (Throwable th) {
						AtomicInteger stats = failedStats.computeIfAbsent(workflowId, s -> new AtomicInteger(0));
						int failed = stats.incrementAndGet();
						if (failed <= 3) {
							workflowQueue.add(workflowId);
						} else {
							logger.error("Repeated (!!!) error " + th.getMessage() + " occurred for " + workflowId + ". Excluding from migration!", th);
						}
					}
				} else {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			logger.info("No workflows left. Finishing " + Thread.currentThread().getName());
			latch.countDown();
		};

		IntStream.range(0, config.queueWorkers()).forEach(o -> {
			Thread thread = new Thread(runnable);
			thread.setName("worker-" + o);
			thread.start();
		});
	}

	private void grabWorkflows() throws IOException {
		try (RestHighLevelClient client = buildEsClient()) {
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(QueryBuilders.matchAllQuery());
			sourceBuilder.size(config.batchSize());
			sourceBuilder.fetchSource(false);

			SearchRequest searchRequest = new SearchRequest();
			searchRequest.scroll(new Scroll(TimeValue.timeValueHours(1L)));
			searchRequest.indices(config.rootIndexName() + ".runtime." + config.env() + ".workflow");
			searchRequest.types("workflow");
			searchRequest.source(sourceBuilder);

			SearchResponse searchResponse = client.search(searchRequest);
			String scrollId = searchResponse.getScrollId();
			SearchHit[] searchHits = searchResponse.getHits().getHits();

			esTotalWorkflows = searchResponse.getHits().getTotalHits();
			logger.info("Workflows total " + esTotalWorkflows);
			AtomicLong retrieved = new AtomicLong(0);
			try {
				while (searchHits != null && searchHits.length > 0) {
					logger.info("Retrieved " + retrieved.addAndGet(searchHits.length) + " of " + esTotalWorkflows);
					for (SearchHit hit : searchHits) {
						workflowQueue.add(hit.getId());
					}

					SearchScrollRequest scroll = new SearchScrollRequest(scrollId);
					scroll.scroll(searchRequest.scroll());
					searchResponse = client.searchScroll(scroll);
					scrollId = searchResponse.getScrollId();
					searchHits = searchResponse.getHits().getHits();
				}
			} finally {
				ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
				clearScrollRequest.addScrollId(scrollId);
				client.clearScroll(clearScrollRequest);
			}
		}
	}

	private void waitWorkflows() {
		int size;
		while ((size = workflowQueue.size()) > 0) {
			logger.info("Waiting ... workflows left " + size);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		keepPooling.set(false);
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		try (Connection tx = dataSource.getConnection()) {
			long pgTotal = dao.workflowCount(tx);

			logger.info("Done. ES Workflow Count = " + esTotalWorkflows + ", PG Workflow Count = " + pgTotal);

		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	private void grabQueues() throws Exception {
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.matchAllQuery());
		sourceBuilder.size(config.batchSize());
		sourceBuilder.fetchSource(true);

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.scroll(new Scroll(TimeValue.timeValueHours(1L)));
		searchRequest.indices(config.rootIndexName() + ".queues." + config.env() + ".*");
		searchRequest.source(sourceBuilder);

		List<String> ignored = Arrays.asList("wait", "deciderqueue", "sweeperqueue");
		try (Connection tx = dataSource.getConnection()) {

			tx.setAutoCommit(false);
			try {

				findAll(searchRequest, hit -> {
					if (ignored.contains(hit.getType())) {
						return;
					}

					// otherwise migrate
					Map<String, Object> map = hit.getSourceAsMap();
					String payload = (String) map.get("payload");
					dao.pushMessage(tx, hit.getType(), hit.getId(), payload);
				});

				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}
	}

	private void grabMetadata() throws Exception {
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.matchAllQuery());
		sourceBuilder.size(config.batchSize());
		sourceBuilder.fetchSource(true);

		// workflow_defs
		try (Connection tx = dataSource.getConnection()) {
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices(config.rootIndexName() + ".metadata." + config.env() + ".workflow_defs");
			searchRequest.types("workflowdefs");
			searchRequest.source(sourceBuilder);
			searchRequest.scroll(new Scroll(TimeValue.timeValueHours(1L)));

			tx.setAutoCommit(false);
			try {

				findAll(searchRequest, hit -> {
					WorkflowDef def = dao.convertValue(hit.getSourceAsMap(), WorkflowDef.class);
					if (def == null) {
						logger.error("Couldn't convert " + hit.getSourceAsMap() + " to WorkflowDef ");
					}
					dao.upsertWorkflowDef(tx, def);
				});

				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}

		// task_defs
		try (Connection tx = dataSource.getConnection()) {
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices(config.rootIndexName() + ".metadata." + config.env() + ".task_defs");
			searchRequest.types("taskdefs");
			searchRequest.source(sourceBuilder);
			searchRequest.scroll(new Scroll(TimeValue.timeValueHours(1L)));

			tx.setAutoCommit(false);
			try {

				findAll(searchRequest, hit -> {
					TaskDef def = dao.convertValue(hit.getSourceAsMap(), TaskDef.class);
					if (def == null) {
						logger.error("Couldn't convert " + hit.getSourceAsMap() + " to TaskDef ");
					}
					dao.upsertTaskDef(tx, def);
				});

				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}

		//event_handlers
		try (Connection tx = dataSource.getConnection()) {
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices(config.rootIndexName() + ".metadata." + config.env() + ".event_handlers");
			searchRequest.types("eventhandlers");
			searchRequest.source(sourceBuilder);
			searchRequest.scroll(new Scroll(TimeValue.timeValueHours(1L)));

			tx.setAutoCommit(false);
			try {

				findAll(searchRequest, hit -> {
					EventHandler def = dao.convertValue(hit.getSourceAsMap(), EventHandler.class);
					if (def == null) {
						logger.error("Couldn't convert " + hit.getSourceAsMap() + " to EventHandler ");
					}
					dao.upsertEventHandler(tx, def);
				});

				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}
	}

	private void dataCleanup() throws SQLException {
		try (Connection tx = dataSource.getConnection()) {
			tx.setAutoCommit(false);
			try {
				dao.dataCleanup(tx);
				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}
	}

	private void requeueAsync() throws SQLException {
		try (Connection tx = dataSource.getConnection()) {
			tx.setAutoCommit(false);
			try {
				dao.requeueAsync(tx);
				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}
	}

	private void requeueDecider() throws SQLException {
		try (Connection tx = dataSource.getConnection()) {
			tx.setAutoCommit(false);
			try {
				dao.requeueSweep(tx);
				tx.commit();
			} catch (Exception ex) {
				tx.rollback();
				throw ex;
			}
		}
	}

	private void processWorkflow(String workflowId, Connection tx) throws Exception {
		String indexName = config.rootIndexName() + ".runtime." + config.env() + ".workflow";
		GetRequest request = new GetRequest().index(indexName).type("workflow").id(workflowId);

		try (RestHighLevelClient client = buildEsClient()) {
			GetResponse record = client.get(request);
			if (!record.isExists()) {
				throw new RuntimeException("No workflow found for " + workflowId);
			}

			Workflow workflow = dao.convertValue(record.getSourceAsMap(), Workflow.class);
			dao.upsertWorkflow(tx, workflow);
		}

		processTasks(workflowId, tx);
	}

	private void processTasks(String workflowId, Connection tx) throws Exception {
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(QueryBuilders.termsQuery("workflowInstanceId", workflowId));
		sourceBuilder.size(1); // Some task size is huge and better to fetch them one by one

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(config.rootIndexName() + ".runtime." + config.env() + ".task");
		searchRequest.types("task");
		searchRequest.source(sourceBuilder);
		searchRequest.scroll(new Scroll(TimeValue.timeValueHours(1L)));

		findAll(searchRequest, hit -> {
			Task task = dao.convertValue(hit.getSourceAsMap(), Task.class);
			if (task == null) {
				logger.error("Couldn't convert " + hit.getSourceAsMap() + " to task ");
			}
			dao.upsertTask(tx, task);
		});
	}

	private void findAll(SearchRequest request, SearchHitHandler handler) throws Exception {
		try (RestHighLevelClient client = buildEsClient()) {
			SearchResponse response = client.search(request);
			String scrollId = response.getScrollId();
			SearchHit[] searchHits = response.getHits().getHits();
			try {
				while (searchHits != null && searchHits.length > 0) {
					for (SearchHit hit : searchHits) {
						handler.apply(hit);
					}

					SearchScrollRequest scroll = new SearchScrollRequest(scrollId);
					scroll.scroll(request.scroll());
					response = client.searchScroll(scroll);
					scrollId = response.getScrollId();
					searchHits = response.getHits().getHits();
				}
			} finally {
				ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
				clearScrollRequest.addScrollId(scrollId);
				client.clearScroll(clearScrollRequest);
			}
		}
	}

	private RestHighLevelClient buildEsClient() {
		return new RestHighLevelClient(builder);
	}

}

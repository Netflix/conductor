package com.netflix.conductor.archiver.cleanup;

import com.netflix.conductor.archiver.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class WorkflowCleanup extends AbstractCleanup {
	private static final Logger logger = LogManager.getLogger(WorkflowCleanup.class);
	private BlockingDeque<String> workflowQueue = new LinkedBlockingDeque<>();
	private AtomicBoolean keepPooling = new AtomicBoolean(true);
	private final AppConfig config = AppConfig.getInstance();
	private Set<String> processed = new HashSet<>();
	private CountDownLatch latch = new CountDownLatch(config.queueWorkers());

	public WorkflowCleanup(RestHighLevelClient client) {
		super(client);
	}

	@Override
	public void cleanup() throws IOException {
		long endTime = System.currentTimeMillis() - Duration.ofDays(config.keepDays()).toMillis();
		logger.info("Starting with keepDays " + config.keepDays() + ", endTime " + endTime);

		// Start workers
		startWorkers();

		// Grab root level workflows
		QueryBuilder query = QueryBuilders.boolQuery()
			.must(QueryBuilders.rangeQuery("endTime").lte(endTime))
			.mustNot(QueryBuilders.termsQuery("status", "RESET", "RUNNING"))
			.mustNot(QueryBuilders.existsQuery("parentWorkflowId"));

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(query);
		sourceBuilder.size(config.batchSize());
		sourceBuilder.fetchSource(false);

		Scroll scroll = new Scroll(TimeValue.timeValueMinutes(60L));
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(config.rootIndexName() + ".runtime." + config.env() + ".workflow");
		searchRequest.types("workflow");
		searchRequest.source(sourceBuilder);
		searchRequest.scroll(scroll);

		SearchResponse searchResponse = client.search(searchRequest);
		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		long totalHits = searchResponse.getHits().getTotalHits();
		logger.info("Found " + totalHits + " root level workflows to be purged");
		try {
			while (searchHits != null && searchHits.length > 0) {
				for (SearchHit hit : searchHits) {
					workflowQueue.add(hit.getId());
				}

				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(scroll);
				searchResponse = client.searchScroll(scrollRequest);
				scrollId = searchResponse.getScrollId();
				searchHits = searchResponse.getHits().getHits();
			}
		} finally {
			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			clearScrollRequest.addScrollId(scrollId);
			client.clearScroll(clearScrollRequest);
		}

		// Wait until all are processed
		waitUntilProcessed();
	}

	private void startWorkers() {
		Runnable runnable = () -> {
			while (keepPooling.get() || !workflowQueue.isEmpty()) {
				String workflowId = workflowQueue.poll();
				if (workflowId != null) {
					try {
						processWorkflow(workflowId);
					} catch (Exception e) {
						logger.error(e.getMessage() + " occurred for " + workflowId, e);
					}
				} else {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			logger.info("No workflows left to process. Finishing " + Thread.currentThread().getName());
			latch.countDown();
		};

		IntStream.range(0, config.queueWorkers()).forEach(o -> {
			Thread thread = new Thread(runnable);
			thread.setName("worker-" + o);
			thread.start();
		});
	}

	private void waitUntilProcessed() {
		// Wait for processing
		int size;
		while ((size = workflowQueue.size()) > 0) {
			logger.info("Waiting until all workflows are processed. Workflows left " + size);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		keepPooling.set(false);
		logger.info("Waiting for workers to complete");
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.info("WorkflowPurger done");
	}

	private void processWorkflow(String workflowId) {
		if (processed.contains(workflowId)) {
			logger.debug("Workflow " + workflowId + " already processed");
			return;
		}
		try {
			GetResponse workflow = findOne(config.rootIndexName() + ".runtime." + config.env() + ".workflow",
				"workflow", workflowId);
			if (workflow == null) {
				logger.error("No workflow found for " + workflowId);
			}

			deleteRuntime(workflowId);
			deleteExecutions(workflowId);
			findChildren(workflowId);

			// Delete workflow itself
			if (workflow != null) {
				DeleteRequest deleteRequest = new DeleteRequest();
				deleteRequest.index(workflow.getIndex());
				deleteRequest.type(workflow.getType());
				deleteRequest.id(workflow.getId());

				client.delete(deleteRequest);
			}

			// save WORKFLOW id
			processed.add(workflowId);
		} catch (Exception ex) {
			logger.error("processWorkflow failed with " + ex.getMessage() + " for " + workflowId, ex);
		}
	}

	private void deleteRuntime(String workflowId) {
		try {
			// Find common runtime which all have 'workflowId.keyword' term in their index definition
			QueryBuilder query = QueryBuilders.termQuery("workflowId.keyword", workflowId);

			String[] indices = new String[]{config.rootIndexName() + ".runtime." + config.env() + ".corrid_to_workflow",
				config.rootIndexName() + ".runtime." + config.env() + ".workflow_def_to_workflows",
				config.rootIndexName() + ".runtime." + config.env() + ".workflow_to_tasks",
				config.rootIndexName() + ".runtime." + config.env() + ".workflow_tags",
				config.rootIndexName() + ".runtime." + config.env() + ".scheduled_tasks"};

			// Runtime data
			deleteByRequest(query, indices);

			// Find the task as its index has different term name
			query = QueryBuilders.termQuery("workflowInstanceId", workflowId);
			indices = new String[]{config.rootIndexName() + ".runtime." + config.env() + ".task"};
			deleteByRequest(query, indices);

		} catch (Exception ex) {
			logger.error("findRuntime failed with " + ex.getMessage() + " for " + workflowId, ex);
		}
	}

	// Find task/workflow executions
	private void deleteExecutions(String workflowId) {
		try {
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(QueryBuilders.termQuery("workflowId", workflowId));
			sourceBuilder.size(config.batchSize());

			Scroll scroll = new Scroll(TimeValue.timeValueHours(1L));
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices(config.rootIndexName() + ".executions." + config.env() + ".task",
				config.rootIndexName() + ".executions." + config.env() + ".workflow");
			searchRequest.types("task", "workflow");
			searchRequest.source(sourceBuilder);
			searchRequest.scroll(scroll);

			deleteByRequest(searchRequest);
		} catch (Exception ex) {
			logger.error("findExecutions failed with " + ex.getMessage() + " for " + workflowId, ex);
		}
	}

	private void findChildren(String workflowId) {
		try {
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(QueryBuilders.termQuery("parentWorkflowId", workflowId));
			sourceBuilder.size(config.batchSize());

			Scroll scroll = new Scroll(TimeValue.timeValueHours(1L));
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices(config.rootIndexName() + ".runtime." + config.env() + ".workflow");
			searchRequest.types("workflow");
			searchRequest.source(sourceBuilder);
			searchRequest.scroll(scroll);

			List<SearchHit> hits = findAll(searchRequest);
			for (SearchHit hit : hits) {
				workflowQueue.add(hit.getId());
			}
		} catch (Exception ex) {
			logger.error("findChildren failed with " + ex.getMessage() + " for " + workflowId, ex);
		}
	}
}

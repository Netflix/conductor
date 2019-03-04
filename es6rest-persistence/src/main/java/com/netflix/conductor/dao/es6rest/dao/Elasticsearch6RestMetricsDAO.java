package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.MetricsDAO;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class Elasticsearch6RestMetricsDAO extends Elasticsearch6RestAbstractDAO implements MetricsDAO {
	private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6RestMetricsDAO.class);
	private static final List<Workflow.WorkflowStatus> WORKFLOW_TODAY_STATUSES = Arrays.asList(
		Workflow.WorkflowStatus.COMPLETED,
		Workflow.WorkflowStatus.CANCELLED,
		Workflow.WorkflowStatus.TIMED_OUT,
		Workflow.WorkflowStatus.RUNNING,
		Workflow.WorkflowStatus.FAILED,
		Workflow.WorkflowStatus.RESET
	);
	private static final List<Workflow.WorkflowStatus> WORKFLOW_OVERALL_STATUSES = Arrays.asList(
		Workflow.WorkflowStatus.COMPLETED,
		Workflow.WorkflowStatus.CANCELLED,
		Workflow.WorkflowStatus.TIMED_OUT,
		Workflow.WorkflowStatus.FAILED
	);

	private static final List<String> TASK_NAMES = Arrays.asList(
		"sherlock",
		"sherlock1",
		"sherlock2",
		"waitchecksum",
		"waittranscode",
		"waittransfer",
		"waitsherlock",
		"waitmediaconvert",
		"episodicwaitpending",
		"waitpending");

	private static final List<String> TASK_TYPES = Arrays.asList(
		"HTTP",
		"BATCH",
		"SUB_WORKFLOW");

	private static final List<Task.Status> TASK_STATUSES = Arrays.asList(
		Task.Status.COMPLETED,
		Task.Status.FAILED
	);

	private static final List<String> WORKFLOWS = Arrays.asList(
		"deluxe.dependencygraph.assembly.conformancegroup.process", // Sherlock V1 Assembly Conformance
		"deluxe.dependencygraph.sourcewait.process",                // Sherlock V2 Sourcewait
		"deluxe.dependencygraph.execute.process",                   // Sherlock V2 Execute
		"deluxe.deluxeone.sky.compliance.process",                  // Sky Compliance
		"deluxe.delivery.itune.process"                             // iTune
	);
	private static final AvgAggregationBuilder averageExecTime = AggregationBuilders.avg("aggAvg")
		.script(new Script("doc['endTime'].value != null && doc['endTime'].value > 0 " +
			" && doc['startTime'].value != null && doc['startTime'].value > 0 " +
			" ? doc['endTime'].value - doc['startTime'].value : 0"));

	private static final String prefix = "deluxe.conductor";
	private static final String version = "\\.\\d+\\.\\d+"; // covers '.X.Y' where X and Y any number
	private final MetadataDAO metadataDAO;
	private final String workflowIndex;
	private final String taskIndex;

	@Inject
	public Elasticsearch6RestMetricsDAO(RestHighLevelClient client, Configuration config,
										ObjectMapper mapper, MetadataDAO metadataDAO) {
		super(client, config, mapper, "metrics");
		this.metadataDAO = metadataDAO;
		this.workflowIndex = String.format("conductor.runtime.%s.workflow", config.getStack());
		this.taskIndex = String.format("conductor.runtime.%s.task", config.getStack());
	}

	@Override
	public Map<String, Object> getMetrics() {
		Map<String, AtomicLong> output = new ConcurrentHashMap<>();

		// Filter definitions excluding sub workflows and maintenance workflows
		Set<String> fullNames = metadataDAO.findAll().stream()
			.filter(fullName -> WORKFLOWS.stream().anyMatch(shortName -> fullName.matches(shortName + version)))
			.collect(Collectors.toSet());

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today + overall
			for (boolean today : Arrays.asList(true, false)) {

				// then per each short name
				for (String shortName : WORKFLOWS) {
					// Filter workflow definitions to have current short related only
					Set<String> filtered = fullNames.stream().filter(type -> type.startsWith(shortName)).collect(Collectors.toSet());

					// Workflow counter per short name
					futures.add(pool.submit(() -> workflowCounters(output, today, shortName, filtered)));

					// Workflow average per short name
					futures.add(pool.submit(() -> workflowAverage(output, today, shortName, filtered)));
				}

				// Overall workflow execution average
				futures.add(pool.submit(() -> overallWorkflowAverage(output, today, fullNames)));

				// Task type/refName/status counter
				futures.add(pool.submit(() -> taskTypeRefNameCounters(output, today)));

				// Task type/refName average
				futures.add(pool.submit(() -> taskTypeRefNameAverage(output, today)));

				// Task refNames/status
				//futures.add(pool.submit(() -> taskRefNameCounters(output, today)));

				// Task refName average
				//futures.add(pool.submit(() -> taskRefNameAverage(output, today)));
			}

			// Wait until completed
			for (Future<?> future : futures) {
				try {
					future.get();
				} catch (Exception ex) {
					logger.error("Get future failed " + ex.getMessage(), ex);
				}
			}
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(output);
	}

	private void taskTypeRefNameCounters(Map<String, AtomicLong> map, boolean today) {
		QueryBuilder typeQuery = QueryBuilders.termsQuery("taskType", TASK_TYPES);
		QueryBuilder statusQuery = QueryBuilders.termsQuery("status", TASK_STATUSES);
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typeQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		TermsAggregationBuilder aggregation = AggregationBuilders
			.terms("aggTaskType").field("taskType")
			.subAggregation(AggregationBuilders
				.terms("aggRefName").field("referenceTaskName")
				.subAggregation(AggregationBuilders
					.terms("aggStatus").field("status")
				)
			);

		SearchRequest searchRequest = new SearchRequest(taskIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, aggregation));

		SearchResponse response = search(searchRequest);
		ParsedStringTerms aggTaskType = response.getAggregations().get("aggTaskType");
		for (Object itemType : aggTaskType.getBuckets()) {
			ParsedStringTerms.ParsedBucket typeBucket = (ParsedStringTerms.ParsedBucket) itemType;
			String typeName = typeBucket.getKeyAsString().toLowerCase();

			ParsedStringTerms aggRefName = typeBucket.getAggregations().get("aggRefName");
			for (Object itemName : aggRefName.getBuckets()) {
				ParsedStringTerms.ParsedBucket refBucket = (ParsedStringTerms.ParsedBucket) itemName;
				String refName = refBucket.getKeyAsString().toLowerCase();

				// Init counters. Total per typeName/refName + today/non-today
				initMetric(map, String.format("%s.task_%s_%s", prefix, typeName, refName));
				initMetric(map, String.format("%s.task_%s_%s_today", prefix, typeName, refName));
				// Init counters. Per typeName/refName/status + today/non-today
				for (Task.Status status : TASK_STATUSES) {
					String statusName = status.name().toLowerCase();
					initMetric(map, String.format("%s.task_%s_%s_%s", prefix, typeName, refName, statusName));
					initMetric(map, String.format("%s.task_%s_%s_%s_today", prefix, typeName, refName, statusName));
				}

				// Per each refName/status
				ParsedStringTerms aggStatus = refBucket.getAggregations().get("aggStatus");
				for (Object itemStatus : aggStatus.getBuckets()) {
					ParsedStringTerms.ParsedBucket statusBucket = (ParsedStringTerms.ParsedBucket) itemStatus;
					String statusName = statusBucket.getKeyAsString().toLowerCase();
					long docCount = statusBucket.getDocCount();

					// Parent typeName + refName
					String metricName = String.format("%s.task_%s_%s%s", prefix, typeName, refName, toLabel(today));
					map.get(metricName).addAndGet(docCount);

					// typeName + refName + status
					metricName = String.format("%s.task_%s_%s_%s%s", prefix, typeName, refName, statusName, toLabel(today));
					map.get(metricName).addAndGet(docCount);
				}
			}
		}
	}

	private void taskTypeRefNameAverage(Map<String, AtomicLong> map, boolean today) {
		QueryBuilder typeQuery = QueryBuilders.termsQuery("taskType", TASK_TYPES);
		QueryBuilder statusQuery = QueryBuilders.termsQuery("status", Task.Status.COMPLETED);
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typeQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		TermsAggregationBuilder aggregation = AggregationBuilders
			.terms("aggTaskType").field("taskType")
			.subAggregation(AggregationBuilders
				.terms("aggRefName").field("referenceTaskName")
				.subAggregation(averageExecTime));

		SearchRequest searchRequest = new SearchRequest(taskIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, aggregation));

		SearchResponse response = search(searchRequest);
		ParsedStringTerms aggTaskType = response.getAggregations().get("aggTaskType");
		for (Object item : aggTaskType.getBuckets()) {
			ParsedStringTerms.ParsedBucket typeBucket = (ParsedStringTerms.ParsedBucket) item;
			String typeName = typeBucket.getKeyAsString().toLowerCase();

			ParsedStringTerms aggRefName = typeBucket.getAggregations().get("aggRefName");
			for (Object item2 : aggRefName.getBuckets()) {
				ParsedStringTerms.ParsedBucket refBucket = (ParsedStringTerms.ParsedBucket) item2;
				String refName = refBucket.getKeyAsString().toLowerCase();

				// Init both counters right away if any today/non-today returned
				initMetric(map, String.format("%s.avg_task_execution_msec.%s_%s", prefix, typeName, refName));
				initMetric(map, String.format("%s.avg_task_execution_msec_today.%s_%s", prefix, typeName, refName));

				// Per each refName/status
				ParsedAvg aggAvg = refBucket.getAggregations().get("aggAvg");
				double avg = Double.isInfinite(aggAvg.getValue()) ? 0 : aggAvg.getValue();

				String metricName = String.format("%s.avg_task_execution_msec%s.%s_%s", prefix, toLabel(today), typeName, refName);
				map.put(metricName, new AtomicLong(Math.round(avg)));
			}
		}
	}

	private void taskRefNameAverage(Map<String, AtomicLong> map, boolean today) {
		// Init counters
		for (String refName : TASK_NAMES) {
			String metricName = String.format("%s.avg_task_execution_msec%s.%s", prefix, toLabel(today), refName.toLowerCase());
			map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		}

		QueryBuilder typesQuery = QueryBuilders.termsQuery("referenceTaskName", TASK_NAMES);
		QueryBuilder statusQuery = QueryBuilders.termsQuery("status", Task.Status.COMPLETED);
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typesQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		TermsAggregationBuilder aggregation = AggregationBuilders
			.terms("aggTaskRefName")
			.field("referenceTaskName")
			.subAggregation(averageExecTime);

		SearchRequest searchRequest = new SearchRequest(taskIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, aggregation));

		SearchResponse response = search(searchRequest);
		ParsedStringTerms aggTaskRefName = response.getAggregations().get("aggTaskRefName");
		for (Object item : aggTaskRefName.getBuckets()) {
			ParsedStringTerms.ParsedBucket typeBucket = (ParsedStringTerms.ParsedBucket) item;
			String refName = typeBucket.getKeyAsString().toLowerCase();

			// Per each refName/status
			ParsedAvg aggAvg = typeBucket.getAggregations().get("aggAvg");
			double avg = Double.isInfinite(aggAvg.getValue()) ? 0 : aggAvg.getValue();

			String metricName = String.format("%s.avg_task_execution_msec%s.%s", prefix, toLabel(today), refName);
			map.put(metricName, new AtomicLong(Math.round(avg)));
		}
	}

	private void taskRefNameCounters(Map<String, AtomicLong> map, boolean today) {
		// Init counters
		for (String refName : TASK_NAMES) {
			for (Task.Status status : TASK_STATUSES) {
				String statusName = status.name().toLowerCase();
				String metricName = String.format("%s.task_%s_%s%s", prefix, refName.toLowerCase(), statusName, toLabel(today));
				map.computeIfAbsent(metricName, s -> new AtomicLong(0));
			}
		}

		QueryBuilder typesQuery = QueryBuilders.termsQuery("referenceTaskName", TASK_NAMES);
		QueryBuilder statusQuery = QueryBuilders.termsQuery("status", TASK_STATUSES);
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typesQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		// Aggregation by workflow type and sub aggregation by workflow status
		TermsAggregationBuilder aggregation = AggregationBuilders
			.terms("aggTaskRefName")
			.field("referenceTaskName")
			.subAggregation(AggregationBuilders.terms("aggStatus").field("status"));

		SearchRequest searchRequest = new SearchRequest(taskIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, aggregation));

		SearchResponse response = search(searchRequest);

		ParsedStringTerms aggTaskRefName = response.getAggregations().get("aggTaskRefName");
		for (Object item : aggTaskRefName.getBuckets()) {
			ParsedStringTerms.ParsedBucket typeBucket = (ParsedStringTerms.ParsedBucket) item;
			String refName = typeBucket.getKeyAsString().toLowerCase();

			// Per each refName/status
			ParsedStringTerms aggStatus = typeBucket.getAggregations().get("aggStatus");
			for (Object subBucketItem : aggStatus.getBuckets()) {
				ParsedStringTerms.ParsedBucket statusBucket = (ParsedStringTerms.ParsedBucket) subBucketItem;
				String statusName = statusBucket.getKeyAsString().toLowerCase();
				long docCount = statusBucket.getDocCount();

				String metricName = String.format("%s.task_%s_%s%s", prefix, refName, statusName, toLabel(today));
				map.get(metricName).addAndGet(docCount);
			}
		}
	}

	private void workflowCounters(Map<String, AtomicLong> map, boolean today, String shortName, Set<String> filtered) {
		List<Workflow.WorkflowStatus> workflowStatuses = today ? WORKFLOW_TODAY_STATUSES : WORKFLOW_OVERALL_STATUSES;

		// Init counters
		String metricName = String.format("%s.workflow_started%s", prefix, toLabel(today));
		map.computeIfAbsent(metricName, s -> new AtomicLong(0));

		// Counter name per status
		for (Workflow.WorkflowStatus status : workflowStatuses) {
			metricName = String.format("%s.workflow_%s%s", prefix, status.name().toLowerCase(), toLabel(today));
			map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		}

		// Counter name per workflow type and status
		metricName = String.format("%s.workflow_started%s.%s", prefix, toLabel(today), shortName);
		map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		for (Workflow.WorkflowStatus status : workflowStatuses) {
			metricName = String.format("%s.workflow_%s%s.%s", prefix, status.name().toLowerCase(), toLabel(today), shortName);
			map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		}

		// Build the today/not-today query
		QueryBuilder typeQuery = QueryBuilders.termsQuery("workflowType", filtered);
		QueryBuilder statusQuery = QueryBuilders.termsQuery("status", workflowStatuses);
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typeQuery).must(statusQuery);
		if (today) {
			QueryBuilder startTime = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(startTime);
		}

		// Aggregation by workflow type and sub aggregation by workflow status
		TermsAggregationBuilder aggregation = AggregationBuilders
			.terms("aggWorkflowType")
			.field("workflowType")
			.subAggregation(AggregationBuilders.terms("aggStatus").field("status"));

		SearchRequest searchRequest = new SearchRequest(workflowIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, aggregation));

		SearchResponse response = search(searchRequest);

		ParsedStringTerms countByWorkflow = response.getAggregations().get("aggWorkflowType");
		for (Object item : countByWorkflow.getBuckets()) {
			ParsedStringTerms.ParsedBucket typeBucket = (ParsedStringTerms.ParsedBucket) item;

			// Total started
			metricName = String.format("%s.workflow_started%s", prefix, toLabel(today));
			map.get(metricName).addAndGet(typeBucket.getDocCount());

			// Started per workflow type
			metricName = String.format("%s.workflow_started%s.%s", prefix, toLabel(today), shortName);
			map.get(metricName).addAndGet(typeBucket.getDocCount());

			// Per each workflow/status
			ParsedStringTerms aggStatus = typeBucket.getAggregations().get("aggStatus");
			for (Object subBucketItem : aggStatus.getBuckets()) {
				ParsedStringTerms.ParsedBucket statusBucket = (ParsedStringTerms.ParsedBucket) subBucketItem;
				String statusName = statusBucket.getKeyAsString().toLowerCase();
				long docCount = statusBucket.getDocCount();

				// Counter name per status
				metricName = String.format("%s.workflow_%s%s", prefix, statusName, toLabel(today));
				map.get(metricName).addAndGet(docCount);

				// Counter name per workflow type and status
				metricName = String.format("%s.workflow_%s%s.%s", prefix, statusName, toLabel(today), shortName);
				map.get(metricName).addAndGet(docCount);
			}
		}
	}

	private void workflowAverage(Map<String, AtomicLong> map, boolean today, String shortName, Set<String> filtered) {
		QueryBuilder typesQuery = QueryBuilders.termsQuery("workflowType", filtered);
		QueryBuilder statusQuery = QueryBuilders.termQuery("status", Workflow.WorkflowStatus.COMPLETED);
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typesQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		SearchRequest searchRequest = new SearchRequest(workflowIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, averageExecTime));

		SearchResponse response = search(searchRequest);
		ParsedAvg aggAvg = response.getAggregations().get("aggAvg");

		double avg = Double.isInfinite(aggAvg.getValue()) ? 0 : aggAvg.getValue();

		String metricName = String.format("%s.avg_workflow_execution_sec%s.%s", prefix, toLabel(today), shortName);
		map.put(metricName, new AtomicLong(Math.round(avg / 1000)));
	}

	private void overallWorkflowAverage(Map<String, AtomicLong> map, boolean today, Set<String> fullNames) {
		QueryBuilder typesQuery = QueryBuilders.termsQuery("workflowType", fullNames);
		QueryBuilder statusQuery = QueryBuilders.termQuery("status", "COMPLETED");
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typesQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		SearchRequest searchRequest = new SearchRequest(workflowIndex);
		searchRequest.source(searchSourceBuilder(mainQuery, averageExecTime));

		SearchResponse response = search(searchRequest);
		ParsedAvg aggAvg = response.getAggregations().get("aggAvg");

		double avg = Double.isInfinite(aggAvg.getValue()) ? 0 : aggAvg.getValue();

		String metricName = String.format("%s.avg_workflow_execution_sec%s", prefix, toLabel(today));
		map.put(metricName, new AtomicLong(Math.round(avg / 1000)));
	}

	private SearchResponse search(SearchRequest request) {
		try {
			return client.search(request);
		} catch (IOException e) {
			logger.warn("search failed for " + request + " " + e.getMessage(), e);
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private SearchSourceBuilder searchSourceBuilder(QueryBuilder query, AggregationBuilder aggregation) {
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.aggregation(aggregation);
		sourceBuilder.fetchSource(false);
		sourceBuilder.query(query);
		sourceBuilder.size(0);

		return sourceBuilder;
	}

	private long getPstStartTime() {
		TimeZone pst = TimeZone.getTimeZone("America/Los_Angeles");

		Calendar calendar = new GregorianCalendar();
		calendar.setTimeZone(pst);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		return calendar.getTimeInMillis();
	}

	private void initMetric(Map<String, AtomicLong> map, String metricName) {
		map.computeIfAbsent(metricName, s -> new AtomicLong(0));
	}

	private String toLabel(boolean today) {
		return today ? "_today" : "";
	}
}
package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class Elasticsearch6RestMetricsDAO extends Elasticsearch6RestAbstractDAO implements MetricsDAO {
	private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6RestMetricsDAO.class);
	private static final List<String> WORKFLOWS = Arrays.asList(
		"deluxe.dependencygraph.assembly.conformancegroup.process", // Sherlock V1 Assembly Conformance
		"deluxe.dependencygraph.sourcewait.process",                // Sherlock V2 Sourcewait
		"deluxe.dependencygraph.execute.process",                   // Sherlock V2 Execute
		"deluxe.deluxeone.sky.compliance.process",                  // Sky Compliance
		"deluxe.delivery.itune.process"                             // iTune
	);
	private static final AvgAggregationBuilder workflowExecutionAvg = AggregationBuilders.avg("agg")
		.script(new Script("doc['endTime'].value != null && doc['endTime'].value > 0 " +
			" && doc['startTime'].value != null && doc['startTime'].value > 0 " +
			" ? doc['endTime'].value - doc['startTime'].value : 0"));

	private static final String prefix = "deluxe.conductor";
	private final MetadataDAO metadataDAO;
	private final String workflowIndex;

	@Inject
	public Elasticsearch6RestMetricsDAO(RestHighLevelClient client, Configuration config,
										ObjectMapper mapper, MetadataDAO metadataDAO) {
		super(client, config, mapper, "metrics");
		this.metadataDAO = metadataDAO;
		this.workflowIndex = String.format("conductor.runtime.%s.workflow", config.getStack());
	}

	@Override
	public Map<String, Object> getMetrics() {
		long start = System.currentTimeMillis();
		Map<String, AtomicLong> output = new ConcurrentHashMap<>();

		// Filter definitions excluding sub workflows and maintenance workflows
		Set<String> fullNames = metadataDAO.findAll().stream()
			.filter(fullName -> WORKFLOWS.stream().anyMatch(fullName::startsWith))
			.filter(fullName -> !fullName.contains(".cancel."))
			.filter(fullName -> !fullName.contains(".failed."))
			.filter(fullName -> !fullName.contains(".failedstatus."))
			.collect(Collectors.toSet());

		// total short list size * (2 per counters + 2 per short average) + 2 overall average
		CountDownLatch latch = new CountDownLatch(WORKFLOWS.size() * 4 + 2);

		// today + overall
		for (boolean today : Arrays.asList(true, false)) {
			// then per each short name
			for (String shortName : WORKFLOWS) {
				// Filter types to find current related only
				Set<String> filtered = fullNames.stream().filter(type -> type.startsWith(shortName)).collect(Collectors.toSet());

				// Workflow counter
				new Thread(() -> {
					try {
						getWorkflowCounters(output, today, shortName, filtered);
					} finally {
						latch.countDown();
					}
				}).start();

				// per short name average
				new Thread(() -> {
					try {
						localWorkflowAverage(output, today, shortName, filtered);
					} finally {
						latch.countDown();
					}
				}).start();
			}

			// Overall workflow execution average
			new Thread(() -> {
				try {
					overallWorkflowAverage(output, today, fullNames);
				} finally {
					latch.countDown();
				}
			}).start();
		}

		// wait until completed
		try {
			latch.await();
		} catch (InterruptedException e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		System.out.println("Gather took " + (System.currentTimeMillis() - start) + " msec");
		return new HashMap<>(output);
	}

	private void getWorkflowCounters(Map<String, AtomicLong> map, boolean today, String shortName, Set<String> filtered) {
		// Init counters
		String metricName = String.format("%s.workflow_started%s", prefix, today ? "_today" : "");
		map.computeIfAbsent(metricName, s -> new AtomicLong(0));

		// Counter name per status
		for (Workflow.WorkflowStatus status : Workflow.WorkflowStatus.values()) {
			metricName = String.format("%s.workflow_%s%s", prefix, status.name().toLowerCase(), today ? "_today" : "");
			map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		}

		// Counter name per workflow type and status
		metricName = String.format("%s.workflow_started%s.%s", prefix, today ? "_today" : "", shortName);
		map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		for (Workflow.WorkflowStatus status : Workflow.WorkflowStatus.values()) {
			metricName = String.format("%s.workflow_%s%s.%s", prefix, status.name().toLowerCase(), today ? "_today" : "", shortName);
			map.computeIfAbsent(metricName, s -> new AtomicLong(0));
		}

		// Aggregation by workflow type and sub aggregation by workflow status
		TermsAggregationBuilder aggregationBuilder = AggregationBuilders
			.terms("aggWorkflowType")
			.field("workflowType")
			.subAggregation(AggregationBuilders.terms("aggStatus").field("status"));

		// Build the today/not-today query
		QueryBuilder mainQuery = QueryBuilders.termsQuery("workflowType", filtered);
		if (today) {
			QueryBuilder startTime = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = QueryBuilders.boolQuery().must(mainQuery).must(startTime);
		}

		// Let's find them
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(mainQuery);
		sourceBuilder.aggregation(aggregationBuilder);
		sourceBuilder.fetchSource(false);
		sourceBuilder.size(0);

		SearchRequest searchRequest = new SearchRequest(workflowIndex);
		searchRequest.source(sourceBuilder);

		SearchResponse response = search(searchRequest);

		ParsedStringTerms countByWorkflow = response.getAggregations().get("aggWorkflowType");
		for (Object item : countByWorkflow.getBuckets()) {
			ParsedStringTerms.ParsedBucket typeBucket = (ParsedStringTerms.ParsedBucket) item;

			// Total started
			metricName = String.format("%s.workflow_started%s", prefix, today ? "_today" : "");
			AtomicLong globalCounter = map.computeIfAbsent(metricName, s -> new AtomicLong(0));
			globalCounter.addAndGet(typeBucket.getDocCount());

			// Started per workflow type
			metricName = String.format("%s.workflow_started%s.%s", prefix, today ? "_today" : "", shortName);
			AtomicLong localCounter = map.computeIfAbsent(metricName, s -> new AtomicLong(0));
			localCounter.addAndGet(typeBucket.getDocCount());

			// Per each workflow/status
			ParsedStringTerms aggStatus = typeBucket.getAggregations().get("aggStatus");
			for (Object subBucketItem : aggStatus.getBuckets()) {
				ParsedStringTerms.ParsedBucket statusBucket = (ParsedStringTerms.ParsedBucket) subBucketItem;
				String statusName = statusBucket.getKeyAsString().toLowerCase();
				long docCount = statusBucket.getDocCount();

				// Counter name per status
				metricName = String.format("%s.workflow_%s%s", prefix, statusName, today ? "_today" : "");
				globalCounter = map.computeIfAbsent(metricName, s -> new AtomicLong(0));
				globalCounter.addAndGet(docCount);

				// Counter name per workflow type and status
				metricName = String.format("%s.workflow_%s%s.%s", prefix, statusName, today ? "_today" : "", shortName);
				localCounter = map.computeIfAbsent(metricName, s -> new AtomicLong(0));
				localCounter.addAndGet(docCount);
			}
		}
	}

	//workflow_execution
	private void localWorkflowAverage(Map<String, AtomicLong> map, boolean today, String shortName, Set<String> filtered) {
		// Common queries
		QueryBuilder typesQuery = QueryBuilders.termsQuery("workflowType", filtered);
		QueryBuilder statusQuery = QueryBuilders.termQuery("status", "COMPLETED");
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typesQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		// Let's find them
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.aggregation(workflowExecutionAvg);
		sourceBuilder.query(mainQuery);
		sourceBuilder.fetchSource(false);
		sourceBuilder.size(0);

		SearchRequest searchRequest = new SearchRequest(workflowIndex);
		searchRequest.source(sourceBuilder);

		SearchResponse response = search(searchRequest);
		ParsedAvg agg = response.getAggregations().get("agg");

		double avg = Double.isInfinite(agg.getValue()) ? 0 : agg.getValue();

		String localName = String.format("%s.avg_workflow_execution_sec%s.%s", prefix, today ? "_today" : "", shortName);
		map.put(localName, new AtomicLong(Math.round(avg / 1000)));
	}

	private void overallWorkflowAverage(Map<String, AtomicLong> map, boolean today, Set<String> fullNames) {

		// Build the today/not-today query
		QueryBuilder typesQuery = QueryBuilders.termsQuery("workflowType", fullNames);
		QueryBuilder statusQuery = QueryBuilders.termQuery("status", "COMPLETED");
		BoolQueryBuilder mainQuery = QueryBuilders.boolQuery().must(typesQuery).must(statusQuery);
		if (today) {
			QueryBuilder timeQuery = QueryBuilders.rangeQuery("startTime").gte(getPstStartTime());
			mainQuery = mainQuery.must(timeQuery);
		}

		// Let's find them
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.aggregation(workflowExecutionAvg);
		sourceBuilder.query(mainQuery);
		sourceBuilder.fetchSource(false);
		sourceBuilder.size(0);

		SearchRequest searchRequest = new SearchRequest(workflowIndex);
		searchRequest.source(sourceBuilder);

		SearchResponse response = search(searchRequest);
		ParsedAvg agg = response.getAggregations().get("agg");

		double avg = Double.isInfinite(agg.getValue()) ? 0 : agg.getValue();

		String globalName = String.format("%s.avg_workflow_execution_sec%s", prefix, today ? "_today" : "");
		map.put(globalName, new AtomicLong(Math.round(avg / 1000)));
	}


	private SearchResponse search(SearchRequest request) {
		try {
			return client.search(request);
		} catch (IOException e) {
			logger.warn("search failed for " + request + " " + e.getMessage(), e);
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private String trimWorkflowVersion(String workflowName) {
		Pattern pattern = Pattern.compile("([a-zA-Z_.]+)*");
		Matcher matcher = pattern.matcher(workflowName);

		if (matcher.find()) {
			String match = matcher.group(1);

			if (match.endsWith(".")) {
				match = match.substring(0, match.length() - 1);
			}

			return match;
		} else {
			return workflowName;
		}
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
}
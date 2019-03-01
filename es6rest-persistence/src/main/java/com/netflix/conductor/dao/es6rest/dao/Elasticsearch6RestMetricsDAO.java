package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.MetricsDAO;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
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
	public Map<String, Object> getCounters() {
		return getWorkflowCounters(false);
	}

	@Override
	public Map<String, Object> getTodayCounters() {
		return getWorkflowCounters(true);
	}

	private Map<String, Object> getWorkflowCounters(boolean today) {
		Map<String, AtomicLong> map = new HashMap<>();

		// Names of all the workflows
		List<String> fullNames = metadataDAO.findAll();

		// Filter definitions excluding sub workflows and maintenance workflows
		Set<String> filteredTypes = fullNames.stream()
			.filter(fullName -> WORKFLOWS.stream().anyMatch(fullName::startsWith))
			.filter(fullName -> !fullName.contains(".cancel."))
			.filter(fullName -> !fullName.contains(".failed."))
			.filter(fullName -> !fullName.contains(".failedstatus."))
			.collect(Collectors.toSet());

		// Init counters
		for (String shortName : WORKFLOWS) {
			String globalName = String.format("%s.workflow_started%s", prefix, today ? "_today" : "");
			map.computeIfAbsent(globalName, s -> new AtomicLong(0));

			String localName = String.format("%s.workflow_started%s.%s", prefix, today ? "_today" : "", shortName);
			map.computeIfAbsent(localName, s -> new AtomicLong(0));

			for (Workflow.WorkflowStatus status : Workflow.WorkflowStatus.values()) {
				String statusName = status.name().toLowerCase();

				// Counter name per status
				globalName = String.format("%s.workflow_%s%s", prefix, statusName, today ? "_today" : "");
				map.computeIfAbsent(globalName, s -> new AtomicLong(0));

				// Counter name per workflow type and status
				localName = String.format("%s.workflow_%s%s.%s", prefix, statusName, today ? "_today" : "", shortName);
				map.computeIfAbsent(localName, s -> new AtomicLong(0));
			}
		}

		// Aggregation by workflow type and sub aggregation by workflow status
		TermsAggregationBuilder aggregationBuilder = AggregationBuilders
			.terms("aggWorkflowType")
			.field("workflowType")
			.subAggregation(AggregationBuilders.terms("aggStatus").field("status"));

		// Today means PST start time which is 08 hours behind UTC which is timezone docker containers work
		QueryBuilder mainQuery;
		if (today) {
			TimeZone pst = TimeZone.getTimeZone("America/Los_Angeles");

			Calendar calendar = new GregorianCalendar();
			calendar.setTimeZone(pst);
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);

			QueryBuilder typeQuery = QueryBuilders.termsQuery("workflowType", filteredTypes);
			QueryBuilder startTime = QueryBuilders.rangeQuery("startTime").gte(calendar.getTimeInMillis());
			mainQuery = QueryBuilders.boolQuery().must(typeQuery).must(startTime);
		} else {
			mainQuery = QueryBuilders.termsQuery("workflowType", filteredTypes);
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
			String fullName = typeBucket.getKeyAsString();
			String shortName = WORKFLOWS.stream().filter(fullName::startsWith)
				.findFirst().orElse(trimWorkflowVersion(fullName));

			// Total started
			String globalName = String.format("%s.workflow_started%s", prefix, today ? "_today" : "");
			AtomicLong globalCounter = map.computeIfAbsent(globalName, s -> new AtomicLong(0));
			globalCounter.addAndGet(typeBucket.getDocCount());

			// Started per workflow type
			String localName = String.format("%s.workflow_started%s.%s", prefix, today ? "_today" : "", shortName);
			AtomicLong localCounter = map.computeIfAbsent(localName, s -> new AtomicLong(0));
			localCounter.addAndGet(typeBucket.getDocCount());

			// Per each workflow/status
			ParsedStringTerms aggStatus = typeBucket.getAggregations().get("aggStatus");
			for (Object subBucketItem : aggStatus.getBuckets()) {
				ParsedStringTerms.ParsedBucket statusBucket = (ParsedStringTerms.ParsedBucket) subBucketItem;
				String statusName = statusBucket.getKeyAsString().toLowerCase();
				long docCount = statusBucket.getDocCount();

				// Counter name per status
				globalName = String.format("%s.workflow_%s%s", prefix, statusName, today ? "_today" : "");
				globalCounter = map.computeIfAbsent(globalName, s -> new AtomicLong(0));
				globalCounter.addAndGet(docCount);

				// Counter name per workflow type and status
				localName = String.format("%s.workflow_%s%s.%s", prefix, statusName, today ? "_today" : "", shortName);
				localCounter = map.computeIfAbsent(localName, s -> new AtomicLong(0));
				localCounter.addAndGet(docCount);
			}
		}

		return new HashMap<>(map);
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
}
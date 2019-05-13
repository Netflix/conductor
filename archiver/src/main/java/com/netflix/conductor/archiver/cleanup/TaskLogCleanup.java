package com.netflix.conductor.archiver.cleanup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.archiver.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskLogCleanup extends AbstractCleanup {
	private static final Logger logger = LogManager.getLogger(TaskLogCleanup.class);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMww");

	static {
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	public TaskLogCleanup(RestHighLevelClient client) {
		super(client);
	}

	@Override
	public void cleanup() throws IOException {
		AppConfig config = AppConfig.getInstance();
		long startTime = System.currentTimeMillis() - Duration.ofDays(config.keepDays()).toMillis();
		logger.info("Starting with keepDays " + config.keepDays() + ", startTime " + startTime);

		QueryBuilder created = QueryBuilders.rangeQuery("created").lte(startTime);
		QueryBuilder createdTime = QueryBuilders.rangeQuery("createdTime").lte(startTime);

		String indexPrefix = config.rootIndexName() + "." + config.taskLogPrefix() + ".*";

		findAndDelete(created, indexPrefix + ".event", "event");
		findAndDelete(created, indexPrefix + ".message", "message");
		findAndDelete(createdTime, indexPrefix + ".task", "task");

		logger.info("TaskLogCleanup done");
	}

	private void findAndDelete(QueryBuilder query, String indexName, String typeName) throws IOException {
		logger.info("findAndDelete started for " + indexName + "/" + typeName);
		AppConfig config = AppConfig.getInstance();

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(query);
		sourceBuilder.size(config.batchSize());

		Scroll scroll = new Scroll(TimeValue.timeValueMinutes(30L));
		SearchRequest searchRequest = new SearchRequest(indexName).types(typeName);
		searchRequest.source(sourceBuilder);
		searchRequest.scroll(scroll);

		SearchResponse searchResponse = client.search(searchRequest);
		long totalHits = searchResponse.getHits().getTotalHits();
		logger.info("Found " + totalHits + " records to be purged for " + indexName + "/" + typeName);

		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();

		try {
			while (searchHits != null && searchHits.length > 0) {
				deleteBulk(searchHits);

				totalHits -= searchHits.length;
				logger.info("Waiting until all task logs are processed. Task logs left " + totalHits + " for " + indexName + "/" + typeName);

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
	}

	public void deleteEmptyIndexes() throws IOException {
		logger.info("deleteEmptyIndexes started");
		AppConfig config = AppConfig.getInstance();
		String baseName = config.rootIndexName() + "." + config.taskLogPrefix() + ".*";

		Response response = client.getLowLevelClient().performRequest("GET", "/_cat/indices/" + baseName + "?s=index&format=json");

		if (response.getStatusLine().getStatusCode() != 200) {
			logger.error("Unable to find " + baseName +
				" empty indexes. Response code " + response.getStatusLine().getStatusCode());
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, Object>> indexes = mapper.readValue(response.getEntity().getContent(),
			new TypeReference<List<Map<String, Object>>>() {
			});
		logger.info("Found indexes " + indexes);

		// Get the current week number (matches the index name format. See Indexer DAO) to avoid deleting recently created
		AtomicInteger counter = new AtomicInteger(0);
		String weekNumber = sdf.format(new Date());
		indexes.forEach(index -> {
			String name = (String) index.get("index");
			int count = Integer.parseInt((String) index.get("docs.count"));

			if (count == 0 && !name.contains(weekNumber)) {
				logger.info("Found empty index " + name);

				DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
				deleteIndexRequest.indices(name);

				try {
					client.indices().delete(deleteIndexRequest);
					logger.info("Successfully deleted empty index " + name);
					counter.incrementAndGet();
				} catch (Exception ex) {
					logger.error("Unable to delete index " + name + ". Failed with " + ex.getMessage(), ex);
				}
			}
		});

		if (counter.get() == 0) {
			logger.info("No indexes to be deleted");
		} else {
			logger.info("Successfully deleted " + counter.get() + " empty indexes");
		}
	}
}

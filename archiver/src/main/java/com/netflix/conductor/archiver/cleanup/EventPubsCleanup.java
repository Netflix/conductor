package com.netflix.conductor.archiver.cleanup;

import com.netflix.conductor.archiver.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.time.Duration;

public class EventPubsCleanup extends CommonEventCleanup {
	private static final Logger logger = LogManager.getLogger(EventPubsCleanup.class);

	public EventPubsCleanup(RestHighLevelClient client) {
		super(client);
	}

	@Override
	public void cleanup() throws IOException {
		AppConfig config = AppConfig.getInstance();
		long startTime = System.currentTimeMillis() - Duration.ofDays(config.keepDays()).toMillis();
		logger.info("Starting with keepDays " + config.keepDays() + ", startTime " + startTime);

		QueryBuilder query = QueryBuilders.rangeQuery("published").lte(startTime);

		String indexName = config.rootIndexName() + ".runtime." + config.env() + ".event_published";
		String typeName = "eventpublished";

		cleanup(logger, indexName, typeName, query);
	}
}

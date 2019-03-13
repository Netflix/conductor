package com.netflix.conductor.archiver.export;

import com.netflix.conductor.archiver.config.AppConfig;
import com.netflix.conductor.archiver.writers.EntityWriters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.time.Duration;

public class EventExecsExport extends CommonEventExport {
    private static final Logger logger = LogManager.getLogger(EventExecsExport.class);

    public EventExecsExport(RestHighLevelClient client, EntityWriters writers) {
        super(client, writers);
    }

    @Override
    public void export() throws IOException {
        AppConfig config = AppConfig.getInstance();
        long startTime = System.currentTimeMillis() - Duration.ofDays(config.keepDays()).toMillis();
        logger.info("Starting with keepDays " + config.keepDays() + ", startTime " + startTime);

        QueryBuilder query = QueryBuilders.rangeQuery("created").lte(startTime);

        String indexName = config.rootIndexName() + ".runtime." + config.env() + ".event_execution";
        String typeName = "eventexecution";

        export(logger, indexName, typeName, query);
    }
}

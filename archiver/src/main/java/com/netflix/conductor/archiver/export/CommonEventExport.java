package com.netflix.conductor.archiver.export;

import com.netflix.conductor.archiver.config.AppConfig;
import com.netflix.conductor.archiver.writers.EntityWriters;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;


public abstract class CommonEventExport extends AbstractExport {

    public CommonEventExport(RestHighLevelClient client, EntityWriters writers) {
        super(client, writers);
    }

    protected void export(Logger logger, String index, String type, QueryBuilder query) throws IOException {
        AppConfig config = AppConfig.getInstance();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.size(config.batchSize());

        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(30L));
        SearchRequest searchRequest = new SearchRequest(index).types(type);
        searchRequest.source(sourceBuilder);
        searchRequest.scroll(scroll);

        SearchResponse searchResponse = client.search(searchRequest);
        long totalHits = searchResponse.getHits().getTotalHits();
        logger.info("Found " + totalHits  + " records to be purged");

        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        try {
            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    writers.RUNTIME(convert(wrap(hit)));
                }
                totalHits -= searchHits.length;
                logger.info("Waiting until all events are processed. Events left " + totalHits);

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

        logger.info("EventPurger done");
    }
}

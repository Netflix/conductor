package com.netflix.conductor.archiver.export;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.archiver.config.AppConfig;
import com.netflix.conductor.archiver.writers.EntityWriters;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractExport {
    private ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client;
    EntityWriters writers;

    AbstractExport(RestHighLevelClient client, EntityWriters writers) {
        this.client = client;
        this.writers = writers;
    }

    GetResponse findOne(String indexName, String typeName, String id) throws IOException {
        GetRequest request = new GetRequest().index(indexName).type(typeName).id(id);
        GetResponse record = client.get(request);
        return record.isExists() ? record : null;
    }

    List<SearchHit> findAll(SearchRequest searchRequest) throws IOException {
        List<SearchHit> result = new LinkedList<>();

        SearchResponse searchResponse = client.search(searchRequest);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        try {

            while (searchHits != null && searchHits.length > 0) {
                result.addAll(Arrays.asList(searchHits));

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(searchRequest.scroll());
                searchResponse = client.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            return result;
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest);
        }
    }

    List<SearchHit> findAll(QueryBuilder query, String... indices) throws IOException {
        AppConfig config = AppConfig.getInstance();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.size(config.batchSize());

        // The same as SearchRequest.DEFAULT_INDICES_OPTIONS but true for ignoreUnavailable
        IndicesOptions options = IndicesOptions.fromOptions(true, true,
                true, false,
                true,true, false);

        Scroll scroll = new Scroll(TimeValue.timeValueHours(1L));
        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.source(sourceBuilder);
        searchRequest.scroll(scroll);
        searchRequest.indicesOptions(options);

        return findAll(searchRequest);
    }

    Map<String, Object> wrap(SearchHit hit) {
        return ImmutableMap.of("_index", hit.getIndex(),
                "_type", hit.getType(),
                "_id", hit.getId(),
                "_source", hit.getSourceAsMap());
    }

    Map<String, Object> wrap(GetResponse hit) {
        return ImmutableMap.of("_index", hit.getIndex(),
                "_type", hit.getType(),
                "_id", hit.getId(),
                "_source", hit.getSourceAsMap());
    }

    String convert(Map map) throws JsonProcessingException {
        return mapper.writeValueAsString(map);
    }

    public abstract void export() throws IOException;
}

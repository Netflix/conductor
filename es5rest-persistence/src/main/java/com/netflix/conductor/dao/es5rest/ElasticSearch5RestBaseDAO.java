package com.netflix.conductor.dao.es5rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Oleksiy Lysak
 */
class ElasticSearch5RestBaseDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5RestBaseDAO.class);
    private final static String NAMESPACE_SEP = ".";
    private final static String DEFAULT = "_default_";
    private Set<String> indexCache = ConcurrentHashMap.newKeySet();
    RestHighLevelClient client;
    private ObjectMapper mapper;
    private String context;
    private String prefix;
    private String stack;

    ElasticSearch5RestBaseDAO(RestHighLevelClient client, Configuration config, ObjectMapper mapper, String context) {
        this.client = client;
        this.mapper = mapper;
        this.context = context;

        prefix = config.getProperty("workflow.namespace.prefix", "conductor");
        stack = config.getStack();
    }

    String toIndexName(String... nsValues) {
        StringBuilder builder = new StringBuilder(prefix).append(NAMESPACE_SEP).append(context).append(NAMESPACE_SEP);
        if (StringUtils.isNotEmpty(stack)) {
            builder.append(stack).append(NAMESPACE_SEP);
        }
        for (int i = 0; i < nsValues.length; i++) {
            builder.append(nsValues[i]);
            if (i < nsValues.length - 1) {
                builder.append(NAMESPACE_SEP);
            }
        }
        return builder.toString().toLowerCase();
    }

    String toId(String... nsValues) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nsValues.length; i++) {
            builder.append(nsValues[i]);
            if (i < nsValues.length - 1) {
                builder.append(NAMESPACE_SEP);
            }
        }

        return builder.toString().toLowerCase();
    }

    String toTypeName(String... nsValues) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nsValues.length; i++) {
            builder.append(nsValues[i]);
            if (i < nsValues.length - 1) {
                builder.append(NAMESPACE_SEP);
            }
        }

        // Elastic type name does not allow '_'
        return builder.toString().toLowerCase().replace("_", "");
    }

    private boolean indexExists(String indexName) {
        AtomicBoolean exists = new AtomicBoolean(false);

        // Check index existence first
        doWithRetryNoisy(() -> {
            GetIndexRequest request = new GetIndexRequest().indices(indexName);
            try {
                boolean result = client.indices().exists(request, RequestOptions.DEFAULT);
                exists.set(result);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });

        return exists.get();
    }

    void ensureIndexExists(String indexName) {
        if (indexCache.contains(indexName)) {
            return;
        }
        try {
            boolean exists = indexExists(indexName);

            // Index found. Exiting
            if (exists) {
                indexCache.add(indexName);
                return;
            }

            // Create it otherwise
            doWithRetryNoisy(() -> {
                try {
                    Settings settings = Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .build();

                    CreateIndexRequest request = new CreateIndexRequest(indexName, settings);
                    client.indices().create(request, RequestOptions.DEFAULT);
                    indexCache.add(indexName);
                } catch (Exception ex) {
                    if (ex.getMessage().contains("index_already_exists_exception")) {
                        indexCache.add(indexName);
                    } else {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                }
            });

        } catch (Exception ex) {
            logger.error("ensureIndexExists: failed for {} with {}", indexName, ex.getMessage(), ex);
        }
    }

    void ensureIndexExists(String indexName, String typeName, String ... suffix) {
        if (indexCache.contains(indexName)) {
            return;
        }
        try {
            boolean exists = indexExists(indexName);

            // Index found. Exiting
            if (exists) {
                indexCache.add(indexName);
                return;
            }

            String resourceName = null;
            if (suffix.length > 0) {
                resourceName = "/" + context + "_" + suffix[0] + ".json";
            } else {
                resourceName = "/" + context + "_" + typeName + ".json";
            }

            InputStream stream = getClass().getResourceAsStream(resourceName);
            Map<String, Object> source = mapper.readValue(stream, new TypeReference<Map<String, Object>>() {
            });

            // Means need to replace by type name
            if (source.containsKey(DEFAULT)) {
                Object object = source.get(DEFAULT);
                source.put(typeName, object);
                source.remove(DEFAULT);
            }

            // Create it otherwise
            doWithRetryNoisy(() -> {
                try {
                    Settings settings = Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .build();

                    CreateIndexRequest request = new CreateIndexRequest(indexName, settings)
                            .mapping(typeName, source);

                    client.indices().create(request, RequestOptions.DEFAULT);
                    indexCache.add(indexName);
                } catch (Exception ex) {
                    if (ex.getMessage().contains("index_already_exists_exception")) {
                        indexCache.add(indexName);
                    } else {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                }
            });

        } catch (Exception ex) {
            logger.error("ensureIndexExists: failed for {}/{} with {}", indexName, typeName, ex.getMessage(), ex);
        }
    }

    boolean exists(String indexName, String typeName, String id) {
        ensureIndexExists(indexName);
        try {
            GetResponse record = findOne(indexName, typeName, id);
            return record.isExists();
        } catch (Exception ex) {
            logger.error("exists: failed for {}/{}/{} with {}", indexName, typeName, id, ex.getMessage(), ex);
            throw ex;
        }
    }

    void delete(String indexName, String typeName, String id) {
        ensureIndexExists(indexName);
        try {
            DeleteRequest request = new DeleteRequest()
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .index(indexName)
                    .type(typeName)
                    .id(id);
            doWithRetry(() -> {
                try {
                    client.delete(request, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });
        } catch (Exception ex) {
            logger.error("delete: failed for {}/{}/{} with {}", indexName, typeName, id, ex.getMessage(), ex);
        }
    }

    boolean insert(String indexName, String typeName, String id, Map<String, ?> payload) {
        ensureIndexExists(indexName);
        AtomicBoolean result = new AtomicBoolean(false);

        IndexRequest request = new IndexRequest()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(payload)
                .create(true)
                .index(indexName)
                .type(typeName)
                .id(id);

        doWithRetry(() -> {
            try {
                client.index(request, RequestOptions.DEFAULT);
                result.set(true);
            } catch (Exception ex) {
                if (!ex.getMessage().contains("version_conflict_engine_exception")) {
                    logger.error("insert: failed for {}/{}/{} with {} {}", indexName, typeName, id, ex.getMessage(), toJson(payload), ex);
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            }
        });

        return result.get();
    }

    void update(String indexName, String typeName, String id, Map<String, ?> payload) {
        ensureIndexExists(indexName);

        IndexRequest request = new IndexRequest()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(payload)
                .index(indexName)
                .type(typeName)
                .id(id);

        doWithRetry(() -> {
            try {
                client.index(request, RequestOptions.DEFAULT);
            } catch (Exception ex) {
                if (!ex.getMessage().contains("version_conflict_engine_exception")) {
                    logger.error("update: failed for {}/{}/{} with {} {}", indexName, typeName, id, ex.getMessage(), toJson(payload), ex);
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            }
        });
    }

    GetResponse findOne(String indexName, String typeName, String id) {
        ensureIndexExists(indexName);
        try {
            GetRequest request = new GetRequest().index(indexName).type(typeName).id(id);

            AtomicReference<GetResponse> result = new AtomicReference<>();
            doWithRetryNoisy(() -> {
                try {
                    result.set(client.get(request, RequestOptions.DEFAULT));
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });

            return result.get();
        } catch (Exception ex) {
            logger.error("findOne: failed for {}/{}/{} with {}", indexName, typeName, id, ex.getMessage(), ex);
            throw ex;
        }
    }

    <T> T findOne(String indexName, String typeName, String id, Class<T> clazz) {
        ensureIndexExists(indexName);
        try {
            GetRequest request = new GetRequest().index(indexName).type(typeName).id(id);

            AtomicReference<GetResponse> result = new AtomicReference<>();
            doWithRetryNoisy(() -> {
                try {
                    result.set(client.get(request, RequestOptions.DEFAULT));
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            });

            GetResponse record = result.get();
            if (record.isExists()) {
                return convert(record.getSource(), clazz);
            }
            return null;
        } catch (Exception ex) {
            logger.error("findOne: failed for {}/{}/{}/{} with {}", indexName, typeName, id, clazz, ex.getMessage(), ex);
            throw ex;
        }
    }

    List<String> findIds(String indexName, String typeName) {
        if (logger.isDebugEnabled())
            logger.debug("findIds: index={}, type={}", indexName, typeName);

        // This type of the search fails if no such index
        ensureIndexExists(indexName);
        try {
            List<String> result = new LinkedList<>();

            final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            sourceBuilder.fetchSource(false);
            sourceBuilder.size(1_000);

            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(indexName).types(typeName);
            searchRequest.source(sourceBuilder);
            searchRequest.scroll(scroll);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                for(SearchHit hit : searchHits) {
                    result.add(hit.getId());
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            clearScrollResponse.isSucceeded();

            if (logger.isDebugEnabled())
                logger.debug("findIds: result={}", toJson(result));
            return result;
        } catch (Exception ex) {
            logger.error("findIds: failed for {}/{} with {}", indexName, typeName, ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    <T> List<T> findAll(String indexName, String typeName, QueryBuilder query, Class<T> clazz) {
        if (logger.isDebugEnabled())
            logger.debug("findAll: index={}, type={}, clazz={}", indexName, typeName, clazz);

        // This type of the search fails if no such index
        ensureIndexExists(indexName);
        try {
            List<T> result = new LinkedList<>();

            final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(query);
            sourceBuilder.size(1_000);

            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(indexName).types(typeName);
            searchRequest.source(sourceBuilder);
            searchRequest.scroll(scroll);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                for(SearchHit hit : searchHits) {
                    result.add(convert(hit.getSourceAsMap(), clazz));
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            clearScrollResponse.isSucceeded();

            if (logger.isDebugEnabled())
                logger.debug("findAll: result={}", toJson(result));
            return result;
        } catch (Exception ex) {
            logger.error("findAll: failed for {}/{}/{} with {}", indexName, typeName, clazz, ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    <T> List<T> findAll(String indexName, String typeName, Class<T> clazz) {
        return findAll(indexName, typeName, QueryBuilders.matchAllQuery(), clazz);
    }

    void doWithRetryNoisy(Runnable runnable) {
        int retry = 3;
        while (true) {
            try {
                runnable.run();
                return;
            } catch (Exception ex) {
                retry--;
                if (retry > 0) {
                    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                } else {
                    throw ex;
                }
            }
        }
    }

    private void doWithRetry(Runnable runnable) {
        try {
            doWithRetryNoisy(runnable);
        } catch (Exception ignore) {
        }
    }

    private <T> T convert(Map map, Class<T> clazz) {
        return mapper.convertValue(map, clazz);
    }

    Map<String, ?> toMap(Object value) {
        return mapper.convertValue(value, new TypeReference<Map<String, ?>>() {
        });
    }

    <T> T convert(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    boolean isConflictOrMissingException(Exception ex) {
        return ex.getMessage().contains("document_missing_exception") ||
                ex.getMessage().contains("version_conflict_engine_exception");
    }

}

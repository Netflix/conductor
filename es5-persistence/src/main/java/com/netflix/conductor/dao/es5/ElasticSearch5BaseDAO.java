/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.dao.es5;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.config.Configuration;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5BaseDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5BaseDAO.class);
    private Set<String> indexCache = new ConcurrentSet<>();
    final static String NAMESPACE_SEP = ".";
    private Map<String, Object> defaultMapping;
    protected ObjectMapper mapper;
    protected Client client;
    private String context;
    private String prefix;
    private String stack;

    ElasticSearch5BaseDAO(Client client, Configuration config, ObjectMapper mapper, String context) {
        this.client = client;
        this.mapper = mapper;
        this.context = context;

        prefix = config.getProperty("workflow.namespace.prefix", "conductor");
        stack = config.getStack();

        try {
            InputStream stream = getClass().getResourceAsStream("/default_mapping.json");
            defaultMapping = mapper.readValue(stream, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            logger.error("Unable to load default_mapping.json");
        }
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

    String toId(String ... nsValues) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nsValues.length; i++) {
            builder.append(nsValues[i]);
            if (i < nsValues.length - 1) {
                builder.append(NAMESPACE_SEP);
            }
        }
        return builder.toString();
    }

    String toTypeName(String ... nsValues) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nsValues.length; i++) {
            builder.append(nsValues[i]);
            if (i < nsValues.length - 1) {
                builder.append(NAMESPACE_SEP);
            }
        }
        return builder.toString().replace("_", "");
    }

    void ensureIndexExists(String indexName) {
        if (indexCache.contains(indexName)) {
            return;
        }
        try {
            client.admin().indices().prepareGetIndex().addIndices(indexName).get();
            indexCache.add(indexName);
        } catch (IndexNotFoundException notFound) {
            try {
                client.admin().indices().prepareCreate(indexName).addMapping("_default_", defaultMapping).get();
                indexCache.add(indexName);
            } catch (ResourceAlreadyExistsException ignore) {
                indexCache.add(indexName);
            } catch (Exception ex) {
                logger.error("ensureIndexExists: Failed for " + indexName + " with " + ex.getMessage(), ex);
            }
        }
    }

    boolean exists(String indexName, String typeName, String id) {
        ensureIndexExists(indexName);
        GetResponse record = client.prepareGet(indexName, typeName, id).get();
        return record.isExists();
    }

    void delete(String indexName, String typeName, String id) {
        ensureIndexExists(indexName);
        client.prepareDelete(indexName, typeName, id).get();
    }

    void upsert(String indexName, String typeName, String id, Object payload) {
        ensureIndexExists(indexName);
        client.prepareUpdate(indexName, typeName, id)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setDocAsUpsert(true)
                .setDoc(toMap(payload))
                .get();
    }

    void insert(String indexName, String typeName, String id, Object payload) {
        ensureIndexExists(indexName);
        client.prepareIndex(indexName, typeName, id)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource(toMap(payload))
                .get();
    }

    <T> T findOne(String indexName, String typeName, String id, Class<T> clazz) {
        ensureIndexExists(indexName);
        GetResponse record = client.prepareGet(indexName, typeName, id).get();
        if (record.isExists()) {
            return convert(record.getSource(), clazz);
        }

        return null;
    }

    <T> T findOne(String indexName, QueryBuilder query, Class<T> clazz) {
        List<T> items = findAll(indexName, query, clazz);
        if (items == null || items.isEmpty()) {
            return null;
        }
        return items.get(0);
    }

    <T> List<T> findAll(String indexName, String typeName, Class<T> clazz) {
        logger.debug("findAll: index={}, type={}, clazz={}", indexName, typeName, clazz);

        // This type of the search fails if no such index
        ensureIndexExists(indexName);
        SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setSize(0).get();

        int size = (int) response.getHits().getTotalHits();
        logger.debug("findAll: found={}", size);
        if (size == 0) {
            return Collections.emptyList();
        }

        response = client.prepareSearch(indexName).setTypes(typeName).setSize(size).get();

        List<T> result = Arrays.stream(response.getHits().getHits())
                .map(hit -> convert(hit.getSource(), clazz))
                .collect(Collectors.toList());

        logger.debug("findAll: result={}", toJson(result));
        return result;
    }

    <T> List<T> findAll(String indexName, QueryBuilder query, Class<T> clazz) {
        logger.debug("findAll: index={}, query={}, clazz={}", indexName, query, clazz);

        // This type of the search fails if no such index
        ensureIndexExists(indexName);
        SearchResponse response = client.prepareSearch(indexName).setQuery(query).setSize(0).get();
        int size = (int) response.getHits().getTotalHits();
        logger.debug("findAll: found={}", size);
        if (size == 0) {
            return Collections.emptyList();
        }

        response = client.prepareSearch(indexName).setQuery(query).setSize(size).get();
        List<T> result = Arrays.stream(response.getHits().getHits())
                .map(item -> convert(item.getSource(), clazz))
                .collect(Collectors.toList());
        logger.debug("findAll: result={}", toJson(result));
        return result;
    }

    <T> List<T> findAll(String indexName, String typeName, QueryBuilder query, Class<T> clazz) {
        logger.debug("findAll: index={}, type={}, query={}, clazz={}", indexName, typeName, query, clazz);

        // This type of the search fails if no such index
        ensureIndexExists(indexName);
        SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(0).get();
        int size = (int) response.getHits().getTotalHits();
        logger.debug("findAll: found={}", size);
        if (size == 0) {
            return Collections.emptyList();
        }

        response = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(size).get();
        List<T> result = Arrays.stream(response.getHits().getHits())
                .map(item -> convert(item.getSource(), clazz))
                .collect(Collectors.toList());
        logger.debug("findAll: result={}", toJson(result));
        return result;
    }

    List<String> findIds(String indexName, QueryBuilder query, int size) {
        logger.debug("findIds: index={}, query={}, size={}", indexName, query, size);

        // This type of the search fails if no such index
        ensureIndexExists(indexName);
        SearchResponse response = client.prepareSearch(indexName).setQuery(query).setSize(size).get();
        List<String> result = Arrays.stream(response.getHits().getHits())
                .map(SearchHit::getId)
                .collect(Collectors.toList());
        logger.debug("findIds: result={}", toJson(result));
        return result;
    }

    Map wrap(Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put("payload", value);
        return map;
    }

    Object unwrap(Map map) {
        return map.get("payload");
    }

    Map<String, ?> toMap(Object value) {
        return mapper.convertValue(value, new TypeReference<Map<String, ?>>() {});
    }

    <T> T convert(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    <T> T convert(Map map, Class<T> clazz) {
        return mapper.convertValue(map, clazz);
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
}

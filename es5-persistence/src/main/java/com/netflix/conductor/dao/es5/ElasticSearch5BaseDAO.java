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
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.Configuration;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5BaseDAO {
	private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5BaseDAO.class);
	private final static String NAMESPACE_SEP = ".";
	private final static String DEFAULT = "_default_";
	protected Client client;
	private Set<String> indexCache = new ConcurrentSet<>();
	private ObjectMapper mapper;
	private String context;
	private String prefix;
	private String stack;

	ElasticSearch5BaseDAO(Client client, Configuration config, ObjectMapper mapper, String context) {
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

	void ensureIndexExists(String indexName) {
		if (indexCache.contains(indexName)) {
			return;
		}
		try {
			doWithRetryNoisy(() -> client.admin().indices().prepareGetIndex().addIndices(indexName).get());
			indexCache.add(indexName);
		} catch (IndexNotFoundException notFound) {
			try {
				doWithRetryNoisy(() -> client.admin().indices().prepareCreate(indexName).get());
				indexCache.add(indexName);
			} catch (ResourceAlreadyExistsException ignore) {
				indexCache.add(indexName);
			} catch (Exception ex) {
				logger.error("ensureIndexExists: failed for {} with {}", indexName, ex.getMessage(), ex);
			}
		}
	}

	void ensureIndexExists(String indexName, String typeName, String ... suffix) {
		if (indexCache.contains(indexName)) {
			return;
		}
		try {
			doWithRetryNoisy(() -> client.admin().indices().prepareGetIndex().addIndices(indexName).get());
			indexCache.add(indexName);
		} catch (IndexNotFoundException notFound) {
			try {
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

				doWithRetryNoisy(() -> client.admin().indices().prepareCreate(indexName).addMapping(typeName, source).get());
				indexCache.add(indexName);
			} catch (ResourceAlreadyExistsException ignore) {
				indexCache.add(indexName);
			} catch (Exception ex) {
				logger.error("ensureIndexExists: failed for {}/{} with {}", indexName, typeName, ex.getMessage(), ex);
			}
		}
	}

	boolean exists(String indexName, String typeName, String id) {
		ensureIndexExists(indexName);
		try {
			GetResponse record = client.prepareGet(indexName, typeName, id).get();
			return record.isExists();
		} catch (Exception ex) {
			logger.error("exists: failed for {}/{}/{} with {}", indexName, typeName, id, ex.getMessage(), ex);
			throw ex;
		}
	}

	void delete(String indexName, String typeName, String id) {
		ensureIndexExists(indexName);
		doWithRetry(() -> {
			try {
				client.prepareDelete(indexName, typeName, id)
						.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
						.get();
			} catch (DocumentMissingException ignore) {
			} catch (Exception ex) {
				logger.error("delete: failed for {}/{}/{} with {}", indexName, typeName, id, ex.getMessage(), ex);
			}
		});
	}

	void upsert(String indexName, String typeName, String id, Map<String, ?> payload) {
		ensureIndexExists(indexName);
		doWithRetry(() -> {
			try {
				client.prepareUpdate(indexName, typeName, id)
						.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
						.setDocAsUpsert(true)
						.setDoc(payload)
						.get();
			} catch (VersionConflictEngineException ignore) {
			} catch (Exception ex) {
				logger.error("upsert: failed for {}/{}/{} with {} {}", indexName, typeName, id, ex.getMessage(), toJson(payload), ex);
			}
		});
	}

	void update(String indexName, String typeName, String id, Map<String, ?> payload) {
		ensureIndexExists(indexName);
		doWithRetryNoisy(() -> {
			try {
				client.prepareUpdate(indexName, typeName, id)
						.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
						.setDoc(payload)
						.get();
			} catch (VersionConflictEngineException ignore) {
			} catch (Exception ex) {
				logger.error("update: failed for {}/{}/{} with {} {}", indexName, typeName, id, ex.getMessage(), toJson(payload), ex);
			}
		});
	}

	boolean insert(String indexName, String typeName, String id, Map<String, ?> payload) {
		ensureIndexExists(indexName);
		AtomicBoolean result = new AtomicBoolean();
		doWithRetry(() -> {
			try {
				client.prepareIndex(indexName, typeName, id)
						.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
						.setSource(payload)
						.setCreate(true)
						.get();
				result.set(true);
			} catch (VersionConflictEngineException ex) {
				result.set(false);
			} catch (Exception ex) {
				logger.error("insert: failed for {}/{}/{} with {} {}", indexName, typeName, id, ex.getMessage(), toJson(payload), ex);
				result.set(false);
				throw ex; // Repeat action
			}
		});
		return result.get();
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

	GetResponse findOne(String indexName, String typeName, String id) {
		ensureIndexExists(indexName);
		try {
			GetRequestBuilder request = client.prepareGet(indexName, typeName, id);

			AtomicReference<GetResponse> reference = new AtomicReference<>();
			doWithRetryNoisy(() -> reference.set(request.get()));

			return reference.get();
		} catch (Exception ex) {
			logger.error("findOne: failed for {}/{}/{} with {}", indexName, typeName, id, ex.getMessage(), ex);
			throw ex;
		}
	}

	<T> T findOne(String indexName, String typeName, String id, Class<T> clazz) {
		ensureIndexExists(indexName);
		try {
			GetRequestBuilder request = client.prepareGet(indexName, typeName, id);

			AtomicReference<GetResponse> reference = new AtomicReference<>();
			doWithRetryNoisy(() -> reference.set(request.get()));

			GetResponse record = reference.get();
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
			SearchRequestBuilder request1 = client.prepareSearch(indexName).setTypes(typeName).setSize(0);

			AtomicReference<SearchResponse> reference = new AtomicReference<>();
			doWithRetryNoisy(() -> reference.set(request1.get()));

			Long size = reference.get().getHits().getTotalHits();
			if (logger.isDebugEnabled())
				logger.debug("findAll: found={}", size);
			if (size == 0) {
				return Collections.emptyList();
			}

			SearchRequestBuilder request2 = client.prepareSearch(indexName).setTypes(typeName).setSize(size.intValue());
			doWithRetryNoisy(() -> reference.set(request2.get()));

			List<String> result = Arrays.stream(reference.get().getHits().getHits())
					.map(SearchHit::getId)
					.collect(Collectors.toList());

			if (logger.isDebugEnabled())
				logger.debug("findIds: result={}", toJson(result));
			return result;
		} catch (Exception ex) {
			logger.error("findIds: failed for {}/{} with {}", indexName, typeName, ex.getMessage(), ex);
			throw ex;
		}
	}

	<T> List<T> findAll(String indexName, String typeName, Class<T> clazz) {
		if (logger.isDebugEnabled())
			logger.debug("findAll: index={}, type={}, clazz={}", indexName, typeName, clazz);

		// This type of the search fails if no such index
		ensureIndexExists(indexName);
		try {
			SearchRequestBuilder request1 = client.prepareSearch(indexName).setTypes(typeName).setSize(0);

			AtomicReference<SearchResponse> reference = new AtomicReference<>();
			doWithRetryNoisy(() -> reference.set(request1.get()));

			Long size = reference.get().getHits().getTotalHits();
			if (logger.isDebugEnabled())
				logger.debug("findAll: found={}", size);
			if (size == 0) {
				return Collections.emptyList();
			}

			SearchRequestBuilder request2 = client.prepareSearch(indexName).setTypes(typeName).setSize(size.intValue());
			doWithRetryNoisy(() -> reference.set(request2.get()));

			List<T> result = Arrays.stream(reference.get().getHits().getHits())
					.map(hit -> convert(hit.getSource(), clazz))
					.collect(Collectors.toList());

			if (logger.isDebugEnabled())
				logger.debug("findAll: result={}", toJson(result));
			return result;
		} catch (Exception ex) {
			logger.error("findAll: failed for {}/{}/{} with {}", indexName, typeName, clazz, ex.getMessage(), ex);
			throw ex;
		}
	}

	<T> List<T> findAll(String indexName, String typeName, QueryBuilder query, Class<T> clazz) {
		if (logger.isDebugEnabled())
			logger.debug("findAll: index={}, type={}, query={}, clazz={}", indexName, typeName, query, clazz);

		// This type of the search fails if no such index
		ensureIndexExists(indexName);
		try {
			SearchRequestBuilder request1 = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(0);

			AtomicReference<SearchResponse> reference = new AtomicReference<>();
			doWithRetryNoisy(() -> reference.set(request1.get()));

			Long size = reference.get().getHits().getTotalHits();
			if (logger.isDebugEnabled())
				logger.debug("findAll: found={}", size);
			if (size == 0) {
				return Collections.emptyList();
			}

			SearchRequestBuilder request2 = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(size.intValue());
			doWithRetryNoisy(() -> reference.set(request2.get()));

			List<T> result = Arrays.stream(reference.get().getHits().getHits())
					.map(item -> convert(item.getSource(), clazz))
					.collect(Collectors.toList());
			if (logger.isDebugEnabled())
				logger.debug("findAll: result={}", toJson(result));
			return result;
		} catch (Exception ex) {
			logger.error("findAll: failed for {}/{}/{}/{} with {}", indexName, typeName, query, clazz, ex.getMessage(), ex);
			throw ex;
		}
	}

	<T> List<T> findAll(String indexName, String typeName, QueryBuilder query, int size, Class<T> clazz) {
		if (logger.isDebugEnabled())
			logger.debug("findAll: index={}, query={}, clazz={}", indexName, query, clazz);

		// This type of the search fails if no such index
		ensureIndexExists(indexName);
		try {
			SearchRequestBuilder request = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(size);

			AtomicReference<SearchResponse> reference = new AtomicReference<>();
			doWithRetryNoisy(() -> reference.set(request.get()));

			List<T> result = Arrays.stream(reference.get().getHits().getHits())
					.map(item -> convert(item.getSource(), clazz))
					.collect(Collectors.toList());
			if (logger.isDebugEnabled())
				logger.debug("findAll: result={}", toJson(result));
			return result;
		} catch (Exception ex) {
			logger.error("findAll: failed for {}/{}/{} with {}", indexName, query, clazz, ex.getMessage(), ex);
			throw ex;
		}
	}

	Map<String, ?> toMap(Object value) {
		return mapper.convertValue(value, new TypeReference<Map<String, ?>>() {
		});
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
}

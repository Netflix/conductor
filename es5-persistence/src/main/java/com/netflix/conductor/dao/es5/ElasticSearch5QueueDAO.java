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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;
import io.netty.util.internal.ConcurrentSet;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5QueueDAO extends ElasticSearch5BaseDAO implements QueueDAO {
	private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5QueueDAO.class);
	private static final ConcurrentSet<String> queues = new ConcurrentSet<>();
	private static final int unackScheduleInMS = 60_000;
	private static final int unackTime = 60_000;
	private static final String QUEUE = "queue";
	private final int stalePeriod;
	private String baseName;

	@Inject
	public ElasticSearch5QueueDAO(Client client, Configuration config, ObjectMapper mapper) {
		super(client, config, mapper, "queues");
		this.baseName = toIndexName();
		this.stalePeriod = config.getIntProperty("workflow.elasticsearch.stale.period.seconds", 60) * 1000;

		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::processUnacks, unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);
	}

	@Override
	public void push(String queueName, String id, long offsetTimeInSecond, int priority) {
		if (logger.isDebugEnabled())
			logger.debug("push: {}/{}/{}", queueName, id, offsetTimeInSecond);
		initQueue(queueName);
		try {
			pushMessage(queueName, id, null, offsetTimeInSecond);
		} catch (Exception ex) {
			logger.error("push: failed for {}/{}/{} with {}", queueName, id, offsetTimeInSecond, ex.getMessage(), ex);
		}
	}

	@Override
	public void push(String queueName, List<Message> messages, int priority) {
		if (logger.isDebugEnabled())
			logger.debug("push: {}/{}", queueName, toJson(messages));
		initQueue(queueName);
		try {
			messages.forEach(message -> pushMessage(queueName, message.getId(), message.getPayload(), 0));
		} catch (Exception ex) {
			logger.error("push: failed for {}/{} with {}", queueName, toJson(messages), ex.getMessage(), ex);
		}
	}

	@Override
	public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond, int priority) {
		if (logger.isDebugEnabled())
			logger.debug("pushIfNotExists: {}/{}/{}", queueName, id, offsetTimeInSecond);
		initQueue(queueName);
		try {
			return pushMessage(queueName, id, null, offsetTimeInSecond);
		} catch (Exception ex) {
			logger.error("pushIfNotExists: failed for {}/{}/{} with {}", queueName, id, offsetTimeInSecond, ex.getMessage(), ex);
			return false;
		}
	}

	@Override
	public List<String> pop(String queueName, int count, int timeout) {
		initQueue(queueName);
		long session = System.nanoTime();
		if (logger.isDebugEnabled())
			logger.debug("pop ({}): {}/{}/{}", session, queueName, count, timeout);
		try {
			String indexName = toIndexName(queueName);
			String typeName = toTypeName(queueName);

			// Read ids. For each: read object, try to lock - if success - add to ids
			long start = System.currentTimeMillis();
			Set<String> foundIds = new HashSet<>();
			QueryBuilder popped = QueryBuilders.termQuery("popped", false);
			QueryBuilder deliverOn = QueryBuilders.rangeQuery("deliverOn").lte(System.currentTimeMillis());
			QueryBuilder query = QueryBuilders.boolQuery().must(popped).must(deliverOn);
			while (foundIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {

				// Find the suitable records
				SearchRequestBuilder request = client.prepareSearch(indexName)
						.addSort("deliverOn", SortOrder.ASC)
						.setTypes(typeName)
						.setVersion(true)
						.setQuery(query)
						.setSize(count);

				AtomicReference<SearchResponse> reference = new AtomicReference<>();
				doWithRetryNoisy(() -> reference.set(request.get()));

				// Walk over all of them and 'lock'
				for (SearchHit record : reference.get().getHits().getHits()) {
					try {
						if (logger.isDebugEnabled())
							logger.debug("pop ({}): attempt for {}/{}", session, queueName, record.getId());
						Map<String, Object> map = new HashMap<>();
						map.put("popped", true);
						map.put("unackOn", System.currentTimeMillis() + unackTime);

						doWithRetryNoisy(() -> client.prepareUpdate(indexName, typeName, record.getId())
								.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
								.setVersion(record.getVersion())
								.setDoc(map)
								.get());

						// Add id to the final collection
						foundIds.add(record.getId());
						if (logger.isDebugEnabled())
							logger.debug("pop ({}): success for {}/{}", session, queueName, record.getId());
					} catch (DocumentMissingException ignore) {
						if (logger.isDebugEnabled())
							logger.debug("pop ({}): got document missing for {}/{}", session, queueName, record.getId());
					} catch (VersionConflictEngineException ignore) {
					} catch (Exception ex) {
						logger.error("pop ({}): unable to execute for {}/{}", session, queueName, record.getId(), ex);
					}
				}

				Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
			}
			if (logger.isDebugEnabled())
				logger.debug("pop ({}): {} result {}", session, queueName, foundIds);

			return Lists.newArrayList(foundIds);
		} catch (Exception ex) {
			logger.error("pop ({}): failed for {} with {}", session, queueName, ex.getMessage(), ex);
		}
		return Collections.emptyList();
	}

	/**
	 * Used by 'conductor' event type subscription. Should lock and read
	 */
	@Override
	public List<Message> pollMessages(String queueName, int count, int timeout) {
		if (logger.isDebugEnabled())
			logger.debug("pollMessages: {}/{}/{}", queueName, count, timeout);
		initQueue(queueName);
		try {
			List<String> ids = pop(queueName, count, timeout);
			List<Message> messages = readMessages(queueName, ids);

			if (logger.isDebugEnabled())
				logger.debug("pollMessages: {} result {}" + queueName, messages);
			return messages;
		} catch (Exception ex) {
			logger.error("pollMessages: failed for {}/{}/{} with {}", queueName, count, timeout, ex.getMessage(), ex);
		}
		return Collections.emptyList();
	}

	@Override
	public void remove(String queueName, String id) {
		if (logger.isDebugEnabled())
			logger.debug("remove: {}/{}", queueName, id);
		initQueue(queueName);
		delete(toIndexName(queueName), toTypeName(queueName), id);
		if (logger.isDebugEnabled())
			logger.debug("remove: done for {}/{}", queueName, id);
	}

	@Override
	public int getSize(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("getSize: " + queueName);
		initQueue(queueName);
		try {
			Long total = client.prepareSearch(toIndexName(queueName)).setTypes(toTypeName(queueName))
					.setSize(0).get().getHits().getTotalHits();
			return total.intValue();
		} catch (Exception ex) {
			logger.error("getSize: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
		return 0;
	}

	@Override
	public boolean ack(String queueName, String id) {
		if (logger.isDebugEnabled())
			logger.debug("ack: {}/{}", queueName, id);
		initQueue(queueName);
		GetResponse record = findMessage(queueName, id);
		if (record.isExists()) {
			delete(toIndexName(queueName), toTypeName(queueName), id);

			if (logger.isDebugEnabled())
				logger.debug("ack: true for {}/{}", queueName, id);
			return true;
		}

		if (logger.isDebugEnabled())
			logger.debug("ack: false for {}/{}", queueName, id);
		return false;
	}

	@Override
	public boolean setUnackTimeout(String queueName, String id, long unackTimeout) {
		if (logger.isDebugEnabled())
			logger.debug("setUnackTimeout: {}/{}/{}", queueName, id, unackTimeout);
		initQueue(queueName);

		try {
			GetResponse record = findMessage(queueName, id);
			if (!record.isExists()) {
				if (logger.isDebugEnabled())
					logger.debug("setUnackTimeout: false for {}/{}/{}", queueName, id, unackTimeout);
				return false;
			}

			Map<String, Object> map = new HashMap<>();
			map.put("popped", true);
			map.put("unackOn", System.currentTimeMillis() + unackTimeout);
			doWithRetryNoisy(() -> client.prepareUpdate(toIndexName(queueName), toTypeName(queueName), record.getId())
					.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
					.setVersion(record.getVersion())
					.setDoc(map)
					.get());
		} catch (VersionConflictEngineException ignore) {
		} catch (Exception ex) {
			logger.error("setUnackTimeout: failed for {}/{}/{} with {}", queueName, id, unackTimeout, ex.getMessage(), ex);
			return false;
		}

		if (logger.isDebugEnabled())
			logger.debug("setUnackTimeout: success for {}/{}/{}", queueName, id, unackTimeout);
		return true;
	}

	@Override
	public void flush(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("flush: {}", queueName);
		initQueue(queueName);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		List<String> ids = findIds(indexName, typeName);
		ids.forEach(id -> delete(indexName, typeName, id));
		if (logger.isDebugEnabled())
			logger.debug("flush: done for {}", queueName);
	}

	@Override
	public Map<String, Long> queuesDetail() {
		Map<String, Long> result = new HashMap<>();
		try {
			SearchResponse response = client.prepareSearch(baseName + "*")
					.addAggregation(AggregationBuilders.terms("countByQueue").field("_index"))
					.setFetchSource(false)
					.setSize(0)
					.get();
			Aggregation aggregation = response.getAggregations().get("countByQueue");
			if (aggregation instanceof StringTerms) {
				StringTerms countByQueue = (StringTerms) aggregation;
				for (StringTerms.Bucket bucket : countByQueue.getBuckets()) {
					result.put(bucket.getKey().toString().replace(baseName, ""), bucket.getDocCount());
				}
			}
		} catch (Exception ex) {
			logger.error("queuesDetail: failed with {}", ex.getMessage(), ex);
		}
		if (logger.isDebugEnabled())
			logger.debug("queuesDetail: result {}", result);
		return result;
	}

	@Override
	public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
		Map<String, Map<String, Map<String, Long>>> result = new HashMap<>();
		try {
			TermsAggregationBuilder agg = AggregationBuilders.terms("countByQueue").field("_index");
			agg.subAggregation(AggregationBuilders.filter("size", QueryBuilders.matchQuery("popped", false)));
			agg.subAggregation(AggregationBuilders.filter("uacked", QueryBuilders.matchQuery("popped", true)));

			SearchResponse response = client.prepareSearch(baseName + "*")
					.setFetchSource(false)
					.addAggregation(agg)
					.setSize(0)
					.get();
			Aggregation aggregation = response.getAggregations().get("countByQueue");
			if (aggregation instanceof StringTerms) {
				StringTerms countByQueue = (StringTerms) aggregation;
				for (StringTerms.Bucket bucket : countByQueue.getBuckets()) {
					String queueName = bucket.getKey().toString().replace(baseName, "");
					InternalFilter size = bucket.getAggregations().get("size");
					InternalFilter uacked = bucket.getAggregations().get("uacked");

					Map<String, Long> sizeAndUacked = new HashMap<>();
					sizeAndUacked.put("size", size.getDocCount());
					sizeAndUacked.put("uacked", uacked.getDocCount());

					Map<String, Map<String, Long>> shardMap = new HashMap<>();
					shardMap.put("a", sizeAndUacked);

					result.put(queueName, shardMap);
				}
			}
		} catch (Exception ex) {
			logger.error("queuesDetailVerbose: failed with {}", ex.getMessage(), ex);
		}

		if (logger.isDebugEnabled())
			logger.debug("queuesDetailVerbose: result {}", result);
		return result;
	}

	@Override
	public void processUnacks(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("processUnacks: {}", queueName);
		initQueue(queueName);
		try {
			String indexName = toIndexName(queueName);
			String typeName = toTypeName(queueName);

			QueryBuilder popped = QueryBuilders.termQuery("popped", true);
			QueryBuilder unackOn = QueryBuilders.rangeQuery("unackOn").lte(System.currentTimeMillis() - stalePeriod);
			QueryBuilder query = QueryBuilders.boolQuery().must(popped).must(unackOn);

			// Find the suitable records
			SearchResponse response = client.prepareSearch(indexName)
					.setTypes(typeName)
					.setVersion(true)
					.setQuery(query)
					.setSize(100) // Batch size
					.get();

			if (logger.isDebugEnabled())
				logger.debug("processUnacks: found {} for {}", response.getHits().totalHits, queueName);

			// Walk over all of them and update back to un-popped
			for (SearchHit record : response.getHits().getHits()) {

				if (logger.isDebugEnabled()) {
					Long recUnackOn = (Long)record.getSource().get("unackOn");
					logger.debug("processUnacks: stale unack {} for {}/{}",
							ISODateTimeFormat.dateTime().withZoneUTC().print(recUnackOn), queueName, record.getId());
				}

				try {
					Map<String, Object> map = new HashMap<>();
					map.put("popped", false);
					map.put("deliverOn", System.currentTimeMillis());
					doWithRetryNoisy(() -> client.prepareUpdate(indexName, typeName, record.getId())
							.setVersion(record.getVersion())
							.setDoc(map).get());
					if (logger.isDebugEnabled())
						logger.debug("processUnacks: success {} for {}", record.getId(), queueName);
				} catch (VersionConflictEngineException ignore) {
				} catch (DocumentMissingException ignore) {
					if (logger.isDebugEnabled())
						logger.debug("processUnacks: got document missing for {}/{}", queueName, record.getId());
				} catch (Exception ex) {
					logger.error("processUnacks: unable to execute for {}/{} with {}",
							queueName, record.getId(), ex.getMessage(), ex);
				}
			}
		} catch (Exception ex) {
			logger.error("processUnacks: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
	}

	private boolean pushMessage(String queueName, String id, String payload, long offsetSeconds) {
		if (logger.isDebugEnabled())
			logger.debug("pushMessage: {}/{}/{}", queueName, id, payload);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);
		try {
			Long deliverOn = System.currentTimeMillis() + (offsetSeconds * 1000);
			Map<String, Object> map = new HashMap<>();
			map.put("popped", false);
			map.put("payload", payload);
			map.put("deliverOn", deliverOn);
			insert(indexName, typeName, id, map);
			return true;
		} catch (Exception ex) {
			logger.error("pushMessage: failed for {}/{}/{} with {}", queueName, id, payload, ex.getMessage(), ex);
			return false;
		}
	}

	private List<Message> readMessages(String queueName, List<String> messageIds) {
		if (messageIds.isEmpty()) return Collections.emptyList();

		IdsQueryBuilder addIds = QueryBuilders.idsQuery();
		addIds.ids().addAll(messageIds);

		SearchRequestBuilder request = client.prepareSearch(toIndexName(queueName))
				.setTypes(toTypeName(queueName))
				.setSize(messageIds.size())
				.setQuery(addIds);

		AtomicReference<SearchResponse> reference = new AtomicReference<>();
		doWithRetryNoisy(() -> reference.set(request.get()));

		if (reference.get().getHits().totalHits != messageIds.size()) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, "readMessages: Could not read all messages for given ids: " + messageIds);
		}

		List<Message> messages = new ArrayList<>(reference.get().getHits().getHits().length);
		for (SearchHit hit : reference.get().getHits().getHits()) {
			Message message = new Message();
			message.setId(hit.getId());
			message.setPayload((String) hit.getSource().get("payload"));
			messages.add(message);
		}
		return messages;
	}

	private void initQueue(String queueName) {
		queues.add(queueName);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);
		ensureIndexExists(indexName, typeName, QUEUE);
	}

	private GetResponse findMessage(String queueName, String id) {
		return findOne(toIndexName(queueName), toTypeName(queueName), id);
	}

	private void processUnacks() {
		queues.forEach(this::processUnacks);
	}
}
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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5QueueDAO extends ElasticSearch5BaseDAO implements QueueDAO {
	private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5QueueDAO.class);
	private static final String ALL = "all";
	private String baseName;

	@Inject
	public ElasticSearch5QueueDAO(Client client, Configuration config, ObjectMapper mapper) {
		super(client, config, mapper, "queues");
		this.baseName = toIndexName(); // It will generate 'conductor.queues.owf-dev' index base name
	}

	@Override
	public void push(String queueName, String id, long offsetTimeInSecond) {
		if (logger.isDebugEnabled())
			logger.debug("push: " + queueName + ", id=" + id + ", offsetTimeInSecond=" + offsetTimeInSecond);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		try {
			pushMessage(queueName, id, null, offsetTimeInSecond);
		} catch (Exception ex) {
			logger.error("push: failed for {} with {}\n{}", queueName, ex.getMessage(), id, ex);
		}
	}

	@Override
	public void push(String queueName, List<Message> messages) {
		if (logger.isDebugEnabled())
			logger.debug("push: " + queueName + ", messages=" + toJson(messages));
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		try {
			messages.forEach(message -> pushMessage(queueName, message.getId(), message.getPayload(), 0));
		} catch (Exception ex) {
			logger.error("push: failed for {} with {}\n{}", queueName, ex.getMessage(), toJson(messages), ex);
		}
	}

	@Override
	public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
		if (logger.isDebugEnabled())
			logger.debug("pushIfNotExists: " + queueName + ", id=" + id + ", offsetTimeInSecond=" + offsetTimeInSecond);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		try {
			if (existsMessage(queueName, id)) {
				return false;
			}
			pushMessage(queueName, id, null, offsetTimeInSecond);
			return true;
		} catch (Exception ex) {
			logger.error("pushIfNotExists: failed for {} with {}\n{}", queueName, ex.getMessage(), id, ex);
		}
		return false;
	}

	@Override
	public List<String> pop(String queueName, int count, int timeout) {
		long session = System.nanoTime();
		if (logger.isDebugEnabled())
			logger.debug("pop (" + session + "): " + queueName + ", count=" + count + ", timeout=" + timeout);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		try {
			// Read ids. For each: read object, try to lock - if success - add to ids
			long start = System.currentTimeMillis();
			Set<String> foundIds = new HashSet<>();
			QueryBuilder popped = QueryBuilders.termQuery("popped", false);
			QueryBuilder deliverOn = QueryBuilders.rangeQuery("deliverOn").lte(System.currentTimeMillis());
			QueryBuilder query = QueryBuilders.boolQuery().must(popped).must(deliverOn);
			while (foundIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {

				// Find the suitable records
				SearchResponse response = client.prepareSearch(indexName)
						.setTypes(typeName)
						.setVersion(true)
						.setQuery(query)
						.setSize(count)
						.get();

				// Walk over all of them and 'lock'
				for (SearchHit record : response.getHits().getHits()) {
					try {
						if (logger.isDebugEnabled())
							logger.debug("pop (" + session + "): attempt for " + queueName + ", id=" + record.getId());
						Map<String, Object> json = new HashMap<>();
						json.put("popped", true);
						json.put("poppedOn", System.currentTimeMillis());
						client.prepareUpdate(indexName, typeName, record.getId())
								.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
								.setVersion(record.getVersion())
								.setDoc(json)
								.get();
						// Add id to the final collection
						foundIds.add(record.getId());
						if (logger.isDebugEnabled())
							logger.debug("pop (" + session + "): success for " + queueName + ", id=" + record.getId());
					} catch (DocumentMissingException ignore) { //TODO Investigate this !
						if (logger.isDebugEnabled())
							logger.debug("pop (" + session + "): got document missing for " + queueName + ", id=" + record.getId() + ". No worries!");
					} catch (VersionConflictEngineException ignore) {
						if (logger.isDebugEnabled())
							logger.debug("pop (" + session + "): got version conflict for " + queueName + ", id=" + record.getId() + ". No worries!");
					} catch (Exception ex) {
						logger.error("pop (" + session + "): unable to execute for " + queueName + ", id=" + record.getId(), ex);
					}
				}

				Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
			}
			if (logger.isDebugEnabled())
				logger.debug("pop (" + session + "): " + queueName + ", result " + foundIds);

			return ImmutableList.copyOf(foundIds);
		} catch (Exception ex) {
			logger.error("pop: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
		return Collections.emptyList();
	}

	/**
	 * Used by 'conductor' event type subscription. Should lock and read
	 */
	@Override
	public List<Message> pollMessages(String queueName, int count, int timeout) {
		if (logger.isDebugEnabled())
			logger.debug("pollMessages: " + queueName + ", count=" + count + ", timeout=" + timeout);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		try {
			List<String> ids = pop(queueName, count, timeout);
			List<Message> messages = readMessages(queueName, ids);

			if (logger.isDebugEnabled())
				logger.debug("pollMessages: " + queueName + ", found " + messages);
			return messages;
		} catch (Exception ex) {
			logger.error("pollMessages: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
		return Collections.emptyList();
	}

	@Override
	public void remove(String queueName, String id) {
		if (logger.isDebugEnabled())
			logger.debug("remove: " + queueName + ", id=" + id);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);
		try {
			client.prepareDelete(indexName, typeName, id)
					.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
					.get();
		} catch (Exception ex) {
			logger.error("remove: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
	}

	@Override
	public int getSize(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("getSize: " + queueName);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		try {
			Long total = client.prepareSearch(indexName).setTypes(typeName).setSize(0).get().getHits().getTotalHits();
			return total.intValue();
		} catch (Exception ex) {
			logger.error("getSize: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
		return 0;
	}

	@Override
	public boolean ack(String queueName, String id) {
		if (logger.isDebugEnabled())
			logger.debug("ack: " + queueName + ", id=" + id);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		if (!existsMessage(queueName, id)) {
			return false;
		}
		removeMessage(queueName, id);
		return true;
	}

	@Override
	public boolean setUnackTimeout(String queueName, String id, long unackTimeout) {
		if (logger.isDebugEnabled())
			logger.debug("setUnackTimeout: " + queueName + ", id=" + id + ", unackTimeout=" + unackTimeout);

		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		GetResponse record = findMessage(queueName, id);
		if (!record.isExists()) {
			return false;
		}
		try {
			Long offsetSeconds = unackTimeout / 1000;
			Long newDeliverOn = System.currentTimeMillis() + unackTimeout;

			Map<String, Object> map = new HashMap<>();
			map.put("popped", false);
			map.put("deliverOn", newDeliverOn);
			map.put("offsetSeconds", offsetSeconds);
			client.prepareUpdate(indexName, typeName, id)
					.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
					.setVersion(record.getVersion())
					.setDoc(map)
					.get();
			if (logger.isDebugEnabled())
				logger.debug("setUnackTimeout: done " + queueName + ", id=" + id + ", unackTimeout=" + unackTimeout + ", version=" + record.getVersion());
		} catch (VersionConflictEngineException ignore) {
			if (logger.isDebugEnabled())
				logger.debug("setUnackTimeout: got version conflict for " + queueName + ", id=" + id + ". No worries!");
		} catch (DocumentMissingException ignore) {
			if (logger.isDebugEnabled())
				logger.debug("setUnackTimeout: got document missing for " + queueName + ", id=" + id + ". No worries!");
		} catch (Exception ex) {
			logger.error("setUnackTimeout: unable to set unack timeout for " + queueName + ", id=" + id + ", unackTimeout=" + unackTimeout, ex);
		}
		return true;
	}

	@Override
	public void flush(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("flush: " + queueName);
		String indexName = toIndexName(queueName);
		ensureIndexExists(indexName, toTypeName(queueName), ALL);
		try {
			DeleteByQueryAction.INSTANCE.newRequestBuilder(client).source(indexName).get();
		} catch (Exception ex) {
			logger.error("flush: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
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
			if (logger.isDebugEnabled())
				logger.debug("queuesDetail: " + result);
		} catch (Exception ex) {
			logger.error("queuesDetail: failed with {}", ex.getMessage(), ex);
		}
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

			if (logger.isDebugEnabled())
				logger.debug("queuesDetailVerbose: " + result);
		} catch (Exception ex) {
			logger.error("queuesDetailVerbose: failed with {}", ex.getMessage(), ex);
		}
		return result;
	}

	@Override
	public void processUnacks(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("processUnacks: " + queueName);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		try {
			QueryBuilder popped = QueryBuilders.termQuery("popped", true);
			QueryBuilder deliverOn = QueryBuilders.rangeQuery("deliverOn").lte(System.currentTimeMillis());
			QueryBuilder query = QueryBuilders.boolQuery().must(popped).must(deliverOn);

			// Find the suitable records
			SearchResponse response = client.prepareSearch(indexName)
					.setTypes(typeName)
					.setQuery(query)
					.setVersion(true)
					.get();

			// Walk over all of them and update back to un-popped
			for (SearchHit record : response.getHits().getHits()) {
				try {
					Map<String, Object> json = new HashMap<>();
					json.put("popped", false);
					client.prepareUpdate(indexName, typeName, record.getId())
							.setVersion(record.getVersion())
							.setDoc(json)
							.get();
				} catch (VersionConflictEngineException ignore) {
					if (logger.isDebugEnabled())
						logger.debug("processUnacks: got version conflict for " + queueName + ", id=" + record.getId() + ". No worries!");
				} catch (DocumentMissingException ignore) {
					if (logger.isDebugEnabled())
						logger.debug("processUnacks: got document missing for " + queueName + ", id=" + record.getId() + ". No worries!");
				} catch (Exception ex) {
					logger.error("processUnacks: unable to execute for " + queueName + ", id=" + record.getId(), ex);
				}
			}
		} catch (Exception ex) {
			logger.error("processUnacks: failed for {} with {}", queueName, ex.getMessage(), ex);
		}
	}

	private GetResponse findMessage(String queueName, String id) {
		return client.prepareGet(toIndexName(queueName), toTypeName(queueName), id).get();
	}

	private boolean existsMessage(String queueName, String id) {
		GetResponse exists = findMessage(queueName, id);
		return exists.isExists();
	}

	private void removeMessage(String queueName, String id) {
		try {
			client.prepareDelete(toIndexName(queueName), toTypeName(queueName), id)
					.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
					.get();
		} catch (Exception ex) {
			logger.error("removeMessage: failed for {} with {}\n", queueName, ex.getMessage(), id, ex);
		}
	}

	private void pushMessage(String queueName, String id, String payload, long offsetSeconds) {
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		GetResponse record = findMessage(queueName, id);
		if (!record.isExists()) {
			try {
				Long currentTime = System.currentTimeMillis();
				Long deliverTime = currentTime + (offsetSeconds * 1000);
				Map<String, Object> map = new HashMap<>();
				map.put("popped", false);
				map.put("payload", payload);
				map.put("createdOn", currentTime);
				map.put("deliverOn", deliverTime);
				map.put("offsetSeconds", offsetSeconds);
				client.prepareIndex(indexName, typeName, id)
						.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
						.setSource(map)
						.setCreate(true)
						.get();
			} catch (Exception ex) {
				logger.error("pushMessage: unable to insert into " + queueName + ", id=" + id + ", payload=" + payload, ex);
			}
		} else {
			try {
				Map<String, Object> map = new HashMap<>();
				map.put("payload", payload);
				client.prepareUpdate(indexName, typeName, id)
						.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
						.setVersion(record.getVersion())
						.setDoc(map)
						.get();
			} catch (VersionConflictEngineException ignore) {
				if (logger.isDebugEnabled())
					logger.debug("pushMessage: got version conflict for " + queueName + ", id=" + id + ". No worries!");
			} catch (DocumentMissingException ignore) {
				if (logger.isDebugEnabled())
					logger.debug("pushMessage: got document missing for " + queueName + ", id=" + id + ". No worries!");
			} catch (Exception ex) {
				logger.error("pushMessage: unable to update " + queueName + ", id=" + id + ", payload=" + payload, ex);
			}
		}
	}

	private List<Message> readMessages(String queueName, List<String> messageIds) {
		if (messageIds.isEmpty()) return Collections.emptyList();

		IdsQueryBuilder addIds = QueryBuilders.idsQuery();
		addIds.ids().addAll(messageIds);

		SearchResponse response = client.prepareSearch(toIndexName(queueName))
				.setTypes(toTypeName(queueName))
				.setSize(messageIds.size())
				.setQuery(addIds)
				.get();
		if (response.getHits().totalHits != messageIds.size()) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, "readMessages: Could not read all messages for given ids: " + messageIds);
		}

		List<Message> messages = new ArrayList<>(response.getHits().getHits().length);
		for (SearchHit hit : response.getHits().getHits()) {
			Message message = new Message();
			message.setId(hit.getId());
			message.setPayload((String) hit.getSource().get("payload"));
			messages.add(message);
		}
		return messages;
	}
}
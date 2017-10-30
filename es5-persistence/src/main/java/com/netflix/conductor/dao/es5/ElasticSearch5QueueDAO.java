/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
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
		this.baseName = toIndexName();
	}

	@Override
	public void push(String queueName, String id, long offsetTimeInSecond) {
		logger.debug("push: " + queueName + ", id=" + id + ", offsetTimeInSecond=" + offsetTimeInSecond);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		pushMessage(queueName, id, null, offsetTimeInSecond);
	}

	@Override
	public void push(String queueName, List<Message> messages) {
		logger.debug("push: " + queueName + ", messages=" + toJson(messages));
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		messages.forEach(message -> pushMessage(queueName, message.getId(), message.getPayload(), 0));
	}

	@Override
	public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
		logger.debug("pushIfNotExists: " + queueName + ", id=" + id + ", offsetTimeInSecond=" + offsetTimeInSecond);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		if (existsMessage(queueName, id)) {
			return false;
		}
		pushMessage(queueName, id, null, offsetTimeInSecond);
		return true;
	}

	@Override
	public List<String> pop(String queueName, int count, int timeout) {
		long session = System.nanoTime();
		logger.debug("pop (" + session + "): " + queueName + ", count=" + count + ", timeout=" + timeout);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		// Read ids. For each: read object, try to lock - if success - add to ids
		long start = System.currentTimeMillis();
		Set<String> foundIds = new HashSet<>();
		TermQueryBuilder poppedFilter = QueryBuilders.termQuery ("popped", false);
		RangeQueryBuilder deliverOnFilter = QueryBuilders.rangeQuery("deliverOn").lte(System.currentTimeMillis());
		while (foundIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
			// Find the suitable records
			SearchResponse response = client.prepareSearch(indexName)
					.setQuery(QueryBuilders.boolQuery().must(poppedFilter).must(deliverOnFilter))
					.setTypes(typeName)
					.setVersion(true)
					.setSize(count)
					.get();

			// Walk over all of them and 'lock'
			for (SearchHit record : response.getHits().getHits()) {
				try {
					logger.debug("pop (" + session + "): attempt for " + queueName + ", id=" + record.getId());
					Map<String, Object> json = new HashMap<>();
					json.put("popped", true);
					json.put("poppedOn", System.currentTimeMillis());
					client.prepareUpdate(indexName, typeName, record.getId())
							.setVersion(record.getVersion())
							.setDoc(json)
							.get();
					// Add id to the final collection
					foundIds.add(record.getId());
					logger.debug("pop (" + session + "): success for " + queueName + ", id=" + record.getId());
				} catch (DocumentMissingException ignore) {
					logger.debug("pop (" + session + "): got document missing for " + queueName + ", id=" + record.getId() + ". No worries!");
				} catch (VersionConflictEngineException ignore) {
					logger.debug("pop (" + session + "): got version conflict for " + queueName + ", id=" + record.getId() + ". No worries!");
				} catch (Exception ex) {
					logger.error("pop (" + session + "): unable to execute for " + queueName + ", id=" + record.getId(), ex);
				}
			}

			Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
		}
		logger.debug("pop (" + session + "): " + queueName + ", result " + foundIds);
		return ImmutableList.copyOf(foundIds);
	}

	/**
	 * Used by 'conductor' event type subscription. Should lock and read
	 */
	@Override
	public List<Message> pollMessages(String queueName, int count, int timeout) {
		logger.debug("pollMessages: " + queueName + ", count=" + count + ", timeout=" + timeout);
		ensureIndexExists(toIndexName(queueName), toTypeName(queueName), ALL);
		List<String> ids = pop(queueName, count, timeout);
		List<Message> messages = readMessages(queueName, ids);

		logger.debug("pollMessages: " + queueName + ", found " + messages);
		return messages;
	}

	@Override
	public void remove(String queueName, String id) {
		logger.debug("remove: " + queueName + ", id=" + id);
		client.prepareDelete(toIndexName(queueName), toTypeName(queueName), id).get();
	}

	@Override
	public int getSize(String queueName) {
		logger.debug("getSize: " + queueName);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		Long total = client.prepareSearch(indexName).setTypes(typeName).setSize(0).get().getHits().getTotalHits();
		return total.intValue();
	}

	@Override
	public boolean ack(String queueName, String id) {
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
		logger.debug("setUnackTimeout: " + queueName + ", id=" + id + ", unackTimeout=" + unackTimeout);

		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		GetResponse record = findMessage(queueName, id);
		if (!record.isExists()) {
			return false;
		}
		try {
			Long createdOn = (Long)record.getSource().get("createdOn");
			Long offsetSeconds = unackTimeout / 1000;
			Long newDeliverOn = createdOn + unackTimeout;

			Map<String, Object> json = new HashMap<>();
			json.put("popped", false);
			json.put("deliverOn", newDeliverOn);
			json.put("offsetSeconds", offsetSeconds);
			client.prepareUpdate(indexName, typeName, id)
					.setVersion(record.getVersion())
					.setDoc(json)
					.get();
			logger.debug("setUnackTimeout: done " + queueName + ", id=" + id + ", unackTimeout=" + unackTimeout + ", version=" +record.getVersion());
		} catch (VersionConflictEngineException ignore) {
			logger.debug("setUnackTimeout: got version conflict for " + queueName + ", id=" + id + ". No worries!");
		} catch (DocumentMissingException ignore) {
			logger.debug("setUnackTimeout: got document missing for " + queueName + ", id=" + id + ". No worries!");
		} catch (Exception ex) {
			logger.error("setUnackTimeout: unable to set unack timeout for " + queueName + ", id=" + id + ", unackTimeout=" + unackTimeout, ex);
			throw ex;
		}
		return true;
	}

	@Override
	public void flush(String queueName) {
		logger.debug("flush: " + queueName);
		String indexName = toIndexName(queueName);
		ensureIndexExists(indexName, toTypeName(queueName), ALL);
		DeleteByQueryAction.INSTANCE.newRequestBuilder(client).source(indexName).get();
	}

	@Override
	public Map<String, Long> queuesDetail() {
		Map<String, Long> result = new HashMap<>();
		SearchResponse response = client.prepareSearch(baseName + "*")
				.addAggregation(AggregationBuilders.terms("countByQueue").field("_index"))
				.setFetchSource(false)
				.setSize(0)
				.get();
		Aggregation aggregation = response.getAggregations().get("countByQueue");
		if (aggregation instanceof StringTerms) {
			StringTerms countByQueue = (StringTerms)aggregation;
			for (StringTerms.Bucket bucket : countByQueue.getBuckets()) {
				result.put(bucket.getKey().toString().replace(baseName, ""), bucket.getDocCount());
			}
		}
		logger.debug("queuesDetail: " + result);
		return result;
	}

	@Override
	public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
		Map<String, Map<String, Map<String, Long>>> result = new HashMap<>();

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

		logger.debug("queuesDetailVerbose: " + result);
		return result;
	}

	@Override
	public void processUnacks(String queueName) {
		logger.debug("processUnacks: " + queueName);
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		ensureIndexExists(indexName, typeName, ALL);

		TermQueryBuilder poppedFilter = QueryBuilders.termQuery ("popped", true);
		RangeQueryBuilder deliverOnFilter = QueryBuilders.rangeQuery("deliverOn").lte(System.currentTimeMillis());

		// Find the suitable records
		SearchResponse response = client.prepareSearch(indexName)
				.setQuery(QueryBuilders.boolQuery().must(poppedFilter).must(deliverOnFilter))
				.setTypes(toTypeName(queueName))
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
				logger.debug("processUnacks: got version conflict for " + queueName + ", id=" + record.getId() + ". No worries!");
			} catch (DocumentMissingException ignore) {
				logger.debug("processUnacks: got document missing for " + queueName + ", id=" + record.getId() + ". No worries!");
			} catch (Exception ex) {
				logger.error("processUnacks: unable to execute for " + queueName + ", id=" + record.getId(), ex);
			}
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
		client.prepareDelete(toIndexName(queueName), toTypeName(queueName), id).get();
	}

	private void pushMessage(String queueName, String id, String payload, long offsetTimeInSecond) {
		String indexName = toIndexName(queueName);
		String typeName = toTypeName(queueName);

		if (StringUtils.isNotEmpty(payload)) {
			System.out.println("payload = " + payload);
		}

		GetResponse record = findMessage(queueName, id);
		if (!record.isExists()) {
			try {
				Long currentTime = System.currentTimeMillis();
				Long deliverTime = currentTime + (offsetTimeInSecond * 1000);
				Map<String, Object> json = new HashMap<>();
				json.put("popped", false);
				json.put("payload", payload);
				json.put("createdOn", currentTime);
				json.put("deliverOn", deliverTime);
				json.put("offsetSeconds", offsetTimeInSecond);
				client.prepareIndex(indexName, typeName, id).setSource(json).get();
			} catch (Exception ex) {
				logger.error("pushMessage: unable to insert into " + queueName + ", id=" + id + ", payload=" + payload, ex);
				throw ex;
			}
		} else {
			try {
				Map<String, Object> json = new HashMap<>();
				json.put("payload", payload);
				client.prepareUpdate(indexName, typeName, id)
						.setVersion(record.getVersion())
						.setDoc(json)
						.get();
			} catch (VersionConflictEngineException ignore) {
				logger.debug("pushMessage: got version conflict for " + queueName + ", id=" + id + ". No worries!");
			} catch (DocumentMissingException ignore) {
				logger.debug("pushMessage: got document missing for " + queueName + ", id=" + id + ". No worries!");
			} catch (Exception ex) {
				logger.error("pushMessage: unable to update " + queueName + ", id=" + id + ", payload=" + payload, ex);
				throw ex;
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
			message.setPayload((String)hit.getSource().get("payload"));
			messages.add(message);
		};

		return messages;
	}
}
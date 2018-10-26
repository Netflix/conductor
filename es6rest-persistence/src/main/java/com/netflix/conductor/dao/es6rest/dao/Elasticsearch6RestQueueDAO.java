package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.QueueDAO;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Oleksiy Lysak
 */
public class Elasticsearch6RestQueueDAO extends Elasticsearch6RestAbstractDAO implements QueueDAO {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6RestQueueDAO.class);
    private static final Set<String> queues = ConcurrentHashMap.newKeySet();
    private static final int unackScheduleInMS = 60_000;
    private static final int unackTime = 60_000;
    private static final String DEFAULT = "default";
    private final int stalePeriod;
    private String baseName;

    @Inject
    public Elasticsearch6RestQueueDAO(RestHighLevelClient client, Configuration config, ObjectMapper mapper) {
        super(client, config, mapper, "queues");
        this.baseName = toIndexName();
        this.stalePeriod = config.getIntProperty("workflow.elasticsearch.stale.period.seconds", 60) * 1000;

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::processUnacks, unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void push(String queueName, String id, long offsetTimeInSecond) {
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
    public void push(String queueName, List<Message> messages) {
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
    public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
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

            // Find the suitable records
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.sort("deliverOn", SortOrder.ASC);
            sourceBuilder.query(query);
            sourceBuilder.version(true);
            sourceBuilder.size(count);

            SearchRequest request = new SearchRequest(indexName).types(typeName);
            request.source(sourceBuilder);

            while (foundIds.size() < count && ((System.currentTimeMillis() - start) < timeout)) {
                AtomicReference<SearchResponse> reference = new AtomicReference<>();
                doWithRetryNoisy(() -> {
                    try {
                        reference.set(client.search(request));
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                });

                // Walk over all of them and 'lock'
                for (SearchHit record : reference.get().getHits().getHits()) {
                    try {
                        if (logger.isDebugEnabled())
                            logger.debug("pop ({}): attempt for {}/{}", session, queueName, record.getId());
                        Map<String, Object> map = new HashMap<>();
                        map.put("popped", true);
                        map.put("unackOn", System.currentTimeMillis() + unackTime);

                        UpdateRequest updateRequest = new UpdateRequest();
                        updateRequest.index(indexName);
                        updateRequest.type(typeName);
                        updateRequest.id(record.getId());
                        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                        updateRequest.version(record.getVersion());
                        updateRequest.doc(map);

                        AtomicBoolean ignoreIdFlag = new AtomicBoolean(false);
                        doWithRetryNoisy(() -> {
                            try {
                                client.update(updateRequest);
                            } catch (Exception ex) {
                                if (isConflictOrMissingException(ex)) {
                                    ignoreIdFlag.set(true);
                                } else {
                                    throw new RuntimeException(ex.getMessage(), ex);
                                }
                            }
                        });

                        // Add id to the final collection
                        if (!ignoreIdFlag.get()) {
                            foundIds.add(record.getId());
                        }
                        if (logger.isDebugEnabled())
                            logger.debug("pop ({}): success for {}/{}", session, queueName, record.getId());
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
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.fetchSource(false);
            sourceBuilder.size(0);

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(toIndexName(queueName));
            searchRequest.types(toTypeName(queueName));
            searchRequest.source(sourceBuilder);

            Long total = client.search(searchRequest).getHits().getTotalHits();
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

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            updateRequest.index(toIndexName(queueName));
            updateRequest.type(toTypeName(queueName));
            updateRequest.id(record.getId());
            updateRequest.version(record.getVersion());
            updateRequest.doc(map);

            doWithRetryNoisy(() -> {
                try {
                    client.update(updateRequest);
                } catch (Exception ex) {
                    if (!isConflictOrMissingException(ex)) {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                }
            });

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
            TermsAggregationBuilder aggregationBuilder = AggregationBuilders
                    .terms("countByQueue")
                    .field("_index");

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.aggregation(aggregationBuilder);
            sourceBuilder.fetchSource(false);
            sourceBuilder.size(0);

            SearchRequest searchRequest = new SearchRequest(baseName + "*");
            searchRequest.source(sourceBuilder);

            SearchResponse response = client.search(searchRequest);

            Aggregation aggregation = response.getAggregations().get("countByQueue");
            if (aggregation instanceof ParsedStringTerms) {
                ParsedStringTerms countByQueue = (ParsedStringTerms) aggregation;
                for (Object item : countByQueue.getBuckets()) {
                    ParsedStringTerms.ParsedBucket bucket = (ParsedStringTerms.ParsedBucket) item;
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
            TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("countByQueue").field("_index");
            aggregationBuilder.subAggregation(AggregationBuilders.filter("size", QueryBuilders.matchQuery("popped", false)));
            aggregationBuilder.subAggregation(AggregationBuilders.filter("uacked", QueryBuilders.matchQuery("popped", true)));

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.aggregation(aggregationBuilder);
            sourceBuilder.fetchSource(false);
            sourceBuilder.size(0);

            SearchRequest searchRequest = new SearchRequest(baseName + "*");
            searchRequest.source(sourceBuilder);

            SearchResponse response = client.search(searchRequest);

            Aggregation aggregation = response.getAggregations().get("countByQueue");
            if (aggregation instanceof ParsedStringTerms) {
                ParsedStringTerms countByQueue = (ParsedStringTerms) aggregation;
                for (Object item : countByQueue.getBuckets()) {
                    ParsedStringTerms.ParsedBucket bucket = (ParsedStringTerms.ParsedBucket) item;

                    String queueName = bucket.getKey().toString().replace(baseName, "");
                    ParsedFilter size = bucket.getAggregations().get("size");
                    ParsedFilter uacked = bucket.getAggregations().get("uacked");

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
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.sort("unackOn", SortOrder.ASC);
            sourceBuilder.query(query);
            sourceBuilder.version(true);
            sourceBuilder.size(100);

            SearchRequest request = new SearchRequest(indexName).types(typeName);
            request.source(sourceBuilder);

            SearchResponse response = client.search(request);

            if (logger.isDebugEnabled())
                logger.debug("processUnacks: found {} for {}", response.getHits().totalHits, queueName);

            // Walk over all of them and update back to un-popped
            for (SearchHit record : response.getHits().getHits()) {

                if (logger.isDebugEnabled()) {
                    Long recUnackOn = (Long) record.getSourceAsMap().get("unackOn");
                    logger.debug("processUnacks: stale unack {} for {}/{}",
                            ISODateTimeFormat.dateTime().withZoneUTC().print(recUnackOn), queueName, record.getId());
                }

                try {
                    Map<String, Object> map = new HashMap<>();
                    map.put("popped", false);
                    map.put("deliverOn", System.currentTimeMillis());

                    UpdateRequest updateRequest = new UpdateRequest();
                    updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    updateRequest.version(record.getVersion());
                    updateRequest.index(indexName);
                    updateRequest.type(typeName);
                    updateRequest.id(record.getId());
                    updateRequest.doc(map);

                    doWithRetryNoisy(() -> {
                        try {
                            client.update(updateRequest);
                        } catch (Exception ex) {
                            if (!isConflictOrMissingException(ex)) {
                                throw new RuntimeException(ex.getMessage(), ex);
                            }
                        }
                    });

                    if (logger.isDebugEnabled())
                        logger.debug("processUnacks: success {} for {}", record.getId(), queueName);
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

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(messageIds.size());
        sourceBuilder.query(addIds);

        SearchRequest request = new SearchRequest(toIndexName(queueName)).types(toTypeName(queueName));
        request.source(sourceBuilder);

        AtomicReference<SearchResponse> reference = new AtomicReference<>();
        doWithRetryNoisy(() -> {
            try {
                reference.set(client.search(request));
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        });

        if (reference.get().getHits().totalHits != messageIds.size()) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, "readMessages: Could not read all messages for given ids: " + messageIds);
        }

        List<Message> messages = new ArrayList<>(reference.get().getHits().getHits().length);
        for (SearchHit hit : reference.get().getHits().getHits()) {
            Message message = new Message();
            message.setId(hit.getId());
            message.setPayload((String) hit.getSourceAsMap().get("payload"));
            messages.add(message);
        }
        return messages;
    }

    private void initQueue(String queueName) {
        queues.add(queueName);
        String indexName = toIndexName(queueName);
        String typeName = toTypeName(queueName);
        ensureIndexExists(indexName, typeName, DEFAULT);
    }

    private GetResponse findMessage(String queueName, String id) {
        return findOne(toIndexName(queueName), toTypeName(queueName), id);
    }

    private void processUnacks() {
        queues.forEach(this::processUnacks);
    }
}
package com.netflix.conductor.dao.queue;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
//import com.netflix.dyno.connectionpool.HashPartitioner;
//import com.netflix.dyno.connectionpool.impl.hash.Murmur3HashPartitioner;
import com.netflix.servo.monitor.Stopwatch;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.sortedset.ZAddParams;

/**
 *
 * @author Viren
 *
 */
public class RedisQueue implements Queue {

    private final Logger logger = LoggerFactory.getLogger(RedisQueue.class);

    private String queueName;

    private String shardName;

    private String messageStoreKeyPrefix;

    private String myQueueShard;

    private String unackShardKeyPrefix;

    private int unackTime = 60;

    private QueueMonitor monitor;

    private ObjectMapper om;

    private JedisPool connPool;

    private JedisPool nonQuorumPool;

    private ScheduledExecutorService schedulerForUnacksProcessing;

    private ScheduledExecutorService schedulerForPrefetchProcessing;

//    private HashPartitioner partitioner = new Murmur3HashPartitioner();

    private int maxHashBuckets = 1024;

    public RedisQueue(String redisKeyPrefix, String queueName, String shardName, int unackTime, JedisPool pool) {
        this(redisKeyPrefix, queueName, shardName, unackTime, unackTime, pool);
    }

    public RedisQueue(String redisKeyPrefix, String queueName, String shardName, int unackScheduleInMS, int unackTime, JedisPool pool) {
        this.queueName = queueName;
        this.shardName = shardName;
        this.messageStoreKeyPrefix = redisKeyPrefix + ".MESSAGE.";
        this.myQueueShard = redisKeyPrefix + ".QUEUE." + queueName + "." + shardName;
        this.unackShardKeyPrefix = redisKeyPrefix + ".UNACK." + queueName + "." + shardName + ".";
        this.unackTime = unackTime;
        this.connPool = pool;
        this.nonQuorumPool = pool;

        ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        om.setSerializationInclusion(Include.NON_NULL);
        om.setSerializationInclusion(Include.NON_EMPTY);
        om.disable(SerializationFeature.INDENT_OUTPUT);

        this.om = om;
        this.monitor = new QueueMonitor(queueName, shardName);

        schedulerForUnacksProcessing = Executors.newScheduledThreadPool(1);
        schedulerForPrefetchProcessing = Executors.newScheduledThreadPool(1);

        schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);

        logger.info(RedisQueue.class.getName() + " is ready to serve " + queueName);

    }

    /**
     *
     * @param nonQuorumPool When using a cluster like Dynomite, which relies on the quorum reads, supply a separate non-quorum read connection for ops like size etc.
     */
    public void setNonQuorumPool(JedisPool nonQuorumPool) {
        this.nonQuorumPool = nonQuorumPool;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public int getUnackTime() {
        return unackTime;
    }

    @Override
    public List<String> push(final List<Message> messages) {

        Stopwatch sw = monitor.start(monitor.push, messages.size());
        Jedis conn = connPool.getResource();
        try {

            Pipeline pipe = conn.pipelined();

            for (Message message : messages) {
                String json = om.writeValueAsString(message);
                pipe.hset(messageStoreKey(message.getId()), message.getId(), json);
                double priority = message.getPriority() / 100.0;
                double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
                pipe.zadd(myQueueShard, score, message.getId());
            }
            pipe.sync();
            pipe.close();

            return messages.stream().map(msg -> msg.getId()).collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            conn.close();
            sw.stop();
        }
    }

    //TODO check shard hash
    private String messageStoreKey(String msgId) {
//        Long hash = partitioner.hash(msgId);
//        long bucket = hash % maxHashBuckets;
        return messageStoreKeyPrefix  + queueName;
    }

    private String unackShardKey(String messageId) {
//        Long hash = partitioner.hash(messageId);
//        long bucket = hash % maxHashBuckets;
        return unackShardKeyPrefix;
    }

    @Override
    public List<Message> peek(final int messageCount) {

        Stopwatch sw = monitor.peek.start();
        Jedis jedis = connPool.getResource();

        try {

            Set<String> ids = peekIds(0, messageCount);
            if (ids == null) {
                return Collections.emptyList();
            }

            List<Message> messages = new LinkedList<Message>();
            for (String id : ids) {
                String json = jedis.hget(messageStoreKey(id), id);
                Message message = om.readValue(json, Message.class);
                messages.add(message);
            }
            return messages;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            jedis.close();
            sw.stop();
        }
    }


    //
    //Note: This implementation does NOT support long polling.  The method itself is synchronized, so implementing long poll could potentially block other threads.
    //When required, the long polling should be implemented on the caller side (ie the broker implementation using the recipe)
    //
    @Override
    public synchronized List<Message> pop(int messageCount, int wait, TimeUnit unit) {

        if (messageCount < 1) {
            return Collections.emptyList();
        }

        Stopwatch sw = monitor.start(monitor.pop, messageCount);

        try {

            List<String> peeked = peekIds(0, messageCount).stream().collect(Collectors.toList());
            List<Message> popped = _pop(peeked);
            return popped;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }

    }

    private List<Message> _pop(List<String> batch) throws Exception {

        double unackScore = Long.valueOf(System.currentTimeMillis() + unackTime).doubleValue();

        List<Message> popped = new LinkedList<>();
        ZAddParams zParams = ZAddParams.zAddParams().nx();

        Jedis jedis = connPool.getResource();
        try {

            Pipeline pipe = jedis.pipelined();
            List<Response<Long>> zadds = new ArrayList<>(batch.size());
            for (int i = 0; i < batch.size(); i++) {
                String msgId = batch.get(i);
                if(msgId == null) {
                    break;
                }
                zadds.add(pipe.zadd(unackShardKey(msgId), unackScore, msgId, zParams));
            }
            pipe.sync();

            int count = zadds.size();
            List<String> zremIds = new ArrayList<>(count);
            List<Response<Long>> zremRes = new LinkedList<>();
            for (int i = 0; i < count; i++) {
                long added = zadds.get(i).get();
                if (added == 0) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Cannot add {} to unack queue shard", batch.get(i));
                    }
                    monitor.misses.increment();
                    continue;
                }
                String id = batch.get(i);
                zremIds.add(id);
                zremRes.add(pipe.zrem(myQueueShard, id));
            }
            pipe.sync();

            List<Response<String>> getRes = new ArrayList<>(count);
            for (int i = 0; i < zremRes.size(); i++) {
                long removed = zremRes.get(i).get();
                if (removed == 0) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Cannot remove {} from queue shard", zremIds.get(i));
                    }
                    monitor.misses.increment();
                    continue;
                }
                getRes.add(pipe.hget(messageStoreKey(zremIds.get(i)), zremIds.get(i)));
            }
            pipe.sync();

            for (int i = 0; i < getRes.size(); i++) {
                String json = getRes.get(i).get();
                if (json == null) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Cannot read payload for {}", zremIds.get(i));
                    }
                    monitor.misses.increment();
                    continue;
                }
                Message msg = om.readValue(json, Message.class);
                msg.setShard(shardName);
                popped.add(msg);
            }
            return popped;
        } finally {
            jedis.close();
        }
    }

    @Override
    public boolean ack(String messageId) {

        Stopwatch sw = monitor.ack.start();
        Jedis jedis = connPool.getResource();

        try {

            Long removed = jedis.zrem(unackShardKey(messageId), messageId);
            if (removed > 0) {
                jedis.hdel(messageStoreKey(messageId), messageId);
                return true;
            }

            return false;

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public void ack(List<Message> messages) {

        Stopwatch sw = monitor.ack.start();
        Jedis jedis = connPool.getResource();
        Pipeline pipe = jedis.pipelined();
        List<Response<Long>> responses = new LinkedList<>();
        try {
            for(Message msg : messages) {
                responses.add(pipe.zrem(unackShardKey(msg.getId()), msg.getId()));
            }
            pipe.sync();
            pipe.close();

            List<Response<Long>> dels = new LinkedList<>();
            for(int i = 0; i < messages.size(); i++) {
                Long removed = responses.get(i).get();
                if (removed > 0) {
                    dels.add(pipe.hdel(messageStoreKey(messages.get(i).getId()), messages.get(i).getId()));
                }
            }
            pipe.sync();
            pipe.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            jedis.close();
            sw.stop();
        }


    }

    @Override
    public boolean setUnackTimeout(String messageId, long timeout) {

        Stopwatch sw = monitor.ack.start();
        Jedis jedis = connPool.getResource();

        try {

            double unackScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue();
            Double score = jedis.zscore(unackShardKey(messageId), messageId);
            if (score != null) {
                jedis.zadd(unackShardKey(messageId), unackScore, messageId);
                return true;
            }

            return false;

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public boolean setTimeout(String messageId, long timeout) {

        Jedis jedis = connPool.getResource();

        try {
            String json = jedis.hget(messageStoreKey(messageId), messageId);
            if (json == null) {
                return false;
            }
            Message message = om.readValue(json, Message.class);
            message.setTimeout(timeout);

            Double score = jedis.zscore(myQueueShard, messageId);
            if (score != null) {
                double priorityd = message.getPriority() / 100.0;
                double newScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue() + priorityd;
                jedis.zadd(myQueueShard, newScore, messageId);
                json = om.writeValueAsString(message);
                jedis.hset(messageStoreKey(message.getId()), message.getId(), json);
                return true;

            }

            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            jedis.close();
        }

    }

    @Override
    public boolean remove(String messageId) {

        Stopwatch sw = monitor.remove.start();
        Jedis jedis = connPool.getResource();

        try {

            jedis.zrem(unackShardKey(messageId), messageId);

            Long removed = jedis.zrem(myQueueShard, messageId);
            Long msgRemoved = jedis.hdel(messageStoreKey(messageId), messageId);

            if (removed > 0 && msgRemoved > 0) {
                return true;
            }

            return false;

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public Message get(String messageId) {

        Stopwatch sw = monitor.get.start();
        Jedis jedis = connPool.getResource();
        try {

            String json = jedis.hget(messageStoreKey(messageId), messageId);
            if (json == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Cannot get the message payload " + messageId);
                }
                return null;
            }

            Message msg = om.readValue(json, Message.class);
            return msg;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public long size() {

        Stopwatch sw = monitor.size.start();
        Jedis jedis = nonQuorumPool.getResource();

        try {
            long size = jedis.zcard(myQueueShard);
            return size;
        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public Map<String, Map<String, Long>> shardSizes() {

        Stopwatch sw = monitor.size.start();
        Map<String, Map<String, Long>> shardSizes = new HashMap<>();
        Jedis jedis = nonQuorumPool.getResource();
        try {

            long size = jedis.zcard(myQueueShard);
            long uacked = 0;
            for(int i = 0; i < maxHashBuckets; i++) {
                String unackShardKey = unackShardKeyPrefix + i;
                uacked += jedis.zcard(unackShardKey);
            }

            Map<String, Long> shardDetails = new HashMap<>();
            shardDetails.put("size", size);
            shardDetails.put("uacked", uacked);
            shardSizes.put(shardName, shardDetails);

            return shardSizes;

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public void clear() {
        Jedis jedis = connPool.getResource();
        try {

            jedis.del(myQueueShard);

            for(int i = 0; i < maxHashBuckets; i++) {
                String unackShardKey = unackShardKeyPrefix + i;
                jedis.del(unackShardKey);

                String  messageStoreKey = messageStoreKeyPrefix + i + "." + queueName;
                jedis.del(messageStoreKey);

            }

        } finally {
            jedis.close();
        }
    }

    private Set<String> peekIds(int offset, int count) {
        Jedis jedis = connPool.getResource();
        try {
            double now = Long.valueOf(System.currentTimeMillis() + 1).doubleValue();
            Set<String> scanned = jedis.zrangeByScore(myQueueShard, 0, now, offset, count);
            return scanned;
        } finally {
            jedis.close();
        }
    }

    public void processUnacks() {
        for(int i = 0; i < maxHashBuckets; i++) {
            String unackShardKey = unackShardKeyPrefix + i;
            processUnacks(unackShardKey);
        }
    }

    private void processUnacks(String unackShardKey) {

        Stopwatch sw = monitor.processUnack.start();
        Jedis jedis = connPool.getResource();

        try {

            do {

                long queueDepth = size();
                monitor.queueDepth.record(queueDepth);

                int batchSize = 1_000;

                double now = Long.valueOf(System.currentTimeMillis()).doubleValue();

                Set<Tuple> unacks = jedis.zrangeByScoreWithScores(unackShardKey, 0, now, 0, batchSize);

                if (unacks.size() > 0) {
                    logger.debug("Adding " + unacks.size() + " messages back to the queue for " + queueName);
                } else {
                    //Nothing more to be processed
                    return;
                }

                for (Tuple unack : unacks) {

                    double score = unack.getScore();
                    String member = unack.getElement();

                    String payload = jedis.hget(messageStoreKey(member), member);
                    if (payload == null) {
                        jedis.zrem(unackShardKey(member), member);
                        continue;
                    }

                    jedis.zadd(myQueueShard, score, member);
                    jedis.zrem(unackShardKey(member), member);
                }

            } while (true);

        } finally {
            jedis.close();
            sw.stop();
        }

    }

    @Override
    public void close() throws IOException {
        schedulerForUnacksProcessing.shutdown();
        schedulerForPrefetchProcessing.shutdown();
        monitor.close();
    }


}

package com.netflix.conductor.dao.queue;


import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.servo.monitor.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.sortedset.ZAddParams;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//import com.netflix.dyno.connectionpool.HashPartitioner;
//import com.netflix.dyno.connectionpool.impl.hash.Murmur3HashPartitioner;

/**
 * @author Viren
 */
public class RedisQueue2 implements Queue {

    private final Logger logger = LoggerFactory.getLogger(RedisQueue2.class);

    private String queueName;

    private List<String> allShards;

    private String shardName;

    private String redisKeyPrefix;

    private String messageStoreKey;

    private String myQueueShard;

    private int unackTime = 60;

    private QueueMonitor monitor;

    private ObjectMapper om;

    private JedisPool connPool;

    private JedisPool nonQuorumPool;

    private ConcurrentLinkedQueue<String> prefetchedIds;

    private ScheduledExecutorService schedulerForUnacksProcessing;

    private ScheduledExecutorService schedulerForPrefetchProcessing;

    private int retryCount = 2;

    public RedisQueue2(String redisKeyPrefix, String queueName, String shardName) {
        this(redisKeyPrefix, queueName, shardName, 60_000);
    }

    public RedisQueue2(String redisKeyPrefix, String queueName,  String shardName, int unackScheduleInMS) {
        Set<String> shards = Sets.newHashSet(shardName);

        this.redisKeyPrefix = redisKeyPrefix;
        this.queueName = queueName;
        this.allShards = shards.stream().collect(Collectors.toList());
        this.shardName = shardName;
        this.messageStoreKey = redisKeyPrefix + ".MESSAGE." + queueName;
        this.myQueueShard = getQueueShardKey(queueName, shardName);

        ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        om.setSerializationInclusion(Include.NON_NULL);
        om.setSerializationInclusion(Include.NON_EMPTY);
        om.disable(SerializationFeature.INDENT_OUTPUT);

        this.om = om;
        this.monitor = new QueueMonitor(queueName, shardName);
        this.prefetchedIds = new ConcurrentLinkedQueue<>();

        schedulerForUnacksProcessing = Executors.newScheduledThreadPool(1);
        schedulerForPrefetchProcessing = Executors.newScheduledThreadPool(1);

        schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);

        logger.info(RedisQueue2.class.getName() + " is ready to serve " + queueName);

    }

    public RedisQueue2 withQuorumConn(JedisPool quorumConn) {
        this.connPool = quorumConn;
        return this;
    }

    public RedisQueue2 withNonQuorumConn(JedisPool nonQuorumConn) {
        this.nonQuorumPool = nonQuorumConn;
        return this;
    }

    public RedisQueue2 withUnackTime(int unackTime) {
        this.unackTime = unackTime;
        return this;
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

        Jedis jedis = connPool.getResource();
        Stopwatch sw = monitor.start(monitor.push, messages.size());

        try {

            execute(() -> {
                for (Message message : messages) {
                    String json = om.writeValueAsString(message);
                    jedis.hset(messageStoreKey, message.getId(), json);
                    double priority = message.getPriority() / 100;
                    double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
                    String shard = getNextShard();
                    String queueShard = getQueueShardKey(queueName, shard);
                    jedis.zadd(queueShard, score, message.getId());
                }
                return messages;
            });

            return messages.stream().map(msg -> msg.getId()).collect(Collectors.toList());

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public List<Message> peek(final int messageCount) {

        Jedis jedis = nonQuorumPool.getResource();
        Stopwatch sw = monitor.peek.start();

        try {

            Set<String> ids = peekIds(0, messageCount);
            if (ids == null) {
                return Collections.emptyList();
            }

            List<Message> msgs = execute(() -> {
                List<Message> messages = new LinkedList<Message>();
                for (String id : ids) {
                    String json = jedis.hget(messageStoreKey, id);
                    Message message = om.readValue(json, Message.class);
                    messages.add(message);
                }
                return messages;
            });

            return msgs;

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public List<Message> pop(int messageCount, int wait, TimeUnit unit) {

        if (messageCount < 1) {
            return Collections.emptyList();
        }

        Stopwatch sw = monitor.start(monitor.pop, messageCount);
        try {
            long start = System.currentTimeMillis();
            long waitFor = unit.toMillis(wait);
            prefetch.addAndGet(messageCount);
            prefetchIds();
            while (prefetchedIds.size() < messageCount && ((System.currentTimeMillis() - start) < waitFor)) {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                prefetchIds();
            }
            return _pop(messageCount);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }

    }

    @VisibleForTesting
    AtomicInteger prefetch = new AtomicInteger(0);

    private void prefetchIds() {

        if (prefetch.get() < 1) {
            return;
        }

        int prefetchCount = prefetch.get();
        Stopwatch sw = monitor.start(monitor.prefetch, prefetchCount);
        try {

            Set<String> ids = peekIds(0, prefetchCount);
            prefetchedIds.addAll(ids);
            prefetch.addAndGet((-1 * ids.size()));
            if (prefetch.get() < 0 || ids.isEmpty()) {
                prefetch.set(0);
            }
        } finally {
            sw.stop();
        }

    }

    private List<Message> _pop(int messageCount) throws Exception {

        Jedis jedis = connPool.getResource();

        double unackScore = Long.valueOf(System.currentTimeMillis() + unackTime).doubleValue();
        String unackQueueName = getUnackKey(queueName, shardName);

        List<Message> popped = new LinkedList<>();
        ZAddParams zParams = ZAddParams.zAddParams().nx();

        try {
            for (; popped.size() != messageCount; ) {
                String msgId = prefetchedIds.poll();
                if (msgId == null) {
                    break;
                }

                long added = jedis.zadd(unackQueueName, unackScore, msgId, zParams);
                if (added == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("cannot add {} to the unack shard ", queueName, msgId);
                    }
                    monitor.misses.increment();
                    continue;
                }

                long removed = jedis.zrem(myQueueShard, msgId);
                if (removed == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("cannot remove {} from the queue shard ", queueName, msgId);
                    }
                    monitor.misses.increment();
                    continue;
                }

                String json = jedis.hget(messageStoreKey, msgId);
                if (json == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cannot get the message payload for {}", msgId);
                    }
                    monitor.misses.increment();
                    continue;
                }
                Message msg = om.readValue(json, Message.class);
                popped.add(msg);

                if (popped.size() == messageCount) {
                    return popped;
                }
            }
        } finally {
            jedis.close();
        }
        return popped;
    }

    @Override
    public boolean ack(String messageId) {

        Jedis jedis = connPool.getResource();
        Stopwatch sw = monitor.ack.start();

        try {

            return execute(() -> {

                for (String shard : allShards) {
                    String unackShardKey = getUnackKey(queueName, shard);
                    Long removed = jedis.zrem(unackShardKey, messageId);
                    if (removed > 0) {
                        jedis.hdel(messageStoreKey, messageId);
                        return true;
                    }
                }
                return false;
            });

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public void ack(List<Message> messages) {
        for (Message message : messages) {
            ack(message.getId());
        }
    }

    @Override
    public boolean setUnackTimeout(String messageId, long timeout) {

        Jedis jedis = connPool.getResource();
        Stopwatch sw = monitor.ack.start();

        try {

            return execute(() -> {
                double unackScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue();
                for (String shard : allShards) {

                    String unackShardKey = getUnackKey(queueName, shard);
                    Double score = jedis.zscore(unackShardKey, messageId);
                    if (score != null) {
                        jedis.zadd(unackShardKey, unackScore, messageId);
                        return true;
                    }
                }
                return false;
            });

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public boolean setTimeout(String messageId, long timeout) {
        Jedis jedis = nonQuorumPool.getResource();
        Jedis jedis2 = connPool.getResource();

        try {
            return execute(() -> {

                String json = jedis.hget(messageStoreKey, messageId);
                if (json == null) {
                    return false;
                }
                Message message = om.readValue(json, Message.class);
                message.setTimeout(timeout);

                for (String shard : allShards) {

                    String queueShard = getQueueShardKey(queueName, shard);
                    Double score = jedis2.zscore(queueShard, messageId);
                    if (score != null) {
                        double priorityd = message.getPriority() / 100;
                        double newScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue() + priorityd;
                        ZAddParams params = ZAddParams.zAddParams().xx();
                        jedis2.zadd(queueShard, newScore, messageId, params);
                        json = om.writeValueAsString(message);
                        jedis2.hset(messageStoreKey, message.getId(), json);
                        return true;
                    }
                }
                return false;
            });

        }finally {
            jedis.close();
            jedis2.close();
        }

    }

    @Override
    public boolean remove(String messageId) {

        Jedis jedis = connPool.getResource();
        Stopwatch sw = monitor.remove.start();

        try {

            return execute(() -> {

                for (String shard : allShards) {

                    String unackShardKey = getUnackKey(queueName, shard);
                    jedis.zrem(unackShardKey, messageId);

                    String queueShardKey = getQueueShardKey(queueName, shard);
                    Long removed = jedis.zrem(queueShardKey, messageId);
                    Long msgRemoved = jedis.hdel(messageStoreKey, messageId);

                    if (removed > 0 && msgRemoved > 0) {
                        return true;
                    }
                }

                return false;

            });

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public Message get(String messageId) {

        Jedis jedis = connPool.getResource();
        Stopwatch sw = monitor.get.start();

        try {

            return execute(() -> {
                String json = jedis.hget(messageStoreKey, messageId);
                if (json == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cannot get the message payload " + messageId);
                    }
                    return null;
                }

                Message msg = om.readValue(json, Message.class);
                return msg;
            });

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public long size() {

        Jedis jedis = nonQuorumPool.getResource();
        Stopwatch sw = monitor.size.start();

        try {

            return execute(() -> {
                long size = 0;
                for (String shard : allShards) {
                    size += jedis.zcard(getQueueShardKey(queueName, shard));
                }
                return size;
            });

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public Map<String, Map<String, Long>> shardSizes() {

        Jedis jedis = nonQuorumPool.getResource();
        Stopwatch sw = monitor.size.start();
        Map<String, Map<String, Long>> shardSizes = new HashMap<>();
        try {

            return execute(() -> {
                for (String shard : allShards) {
                    long size = jedis.zcard(getQueueShardKey(queueName, shard));
                    long uacked = jedis.zcard(getUnackKey(queueName, shard));
                    Map<String, Long> shardDetails = new HashMap<>();
                    shardDetails.put("size", size);
                    shardDetails.put("uacked", uacked);
                    shardSizes.put(shard, shardDetails);
                }
                return shardSizes;
            });

        } finally {
            jedis.close();
            sw.stop();
        }
    }

    @Override
    public void clear() {
        Jedis jedis = connPool.getResource();

        try {
            execute(() -> {
                for (String shard : allShards) {
                    String queueShard = getQueueShardKey(queueName, shard);
                    String unackShard = getUnackKey(queueName, shard);
                    jedis.del(queueShard);
                    jedis.del(unackShard);
                }
                jedis.del(messageStoreKey);
                return null;
            });
        }finally {
            jedis.close();
        }

    }

    private Set<String> peekIds(int offset, int count) {
        Jedis jedis = connPool.getResource();

        try {
            return execute(() -> {
                double now = Long.valueOf(System.currentTimeMillis() + 1).doubleValue();
                Set<String> scanned = jedis.zrangeByScore(myQueueShard, 0, now, offset, count);
                return scanned;
            });
        }finally {
            jedis.close();
        }

    }

    public void processUnacks() {

        Jedis jedis = connPool.getResource();
        Stopwatch sw = monitor.processUnack.start();
        try {

            long queueDepth = size();
            monitor.queueDepth.record(queueDepth);

            execute(() -> {

                int batchSize = 1_000;
                String unackQueueName = getUnackKey(queueName, shardName);

                double now = Long.valueOf(System.currentTimeMillis()).doubleValue();

                Set<Tuple> unacks = jedis.zrangeByScoreWithScores(unackQueueName, 0, now, 0, batchSize);

                if (unacks.size() > 0) {
                    logger.debug("Adding " + unacks.size() + " messages back to the queue for " + queueName);
                }

                for (Tuple unack : unacks) {

                    double score = unack.getScore();
                    String member = unack.getElement();

                    String payload = jedis.hget(messageStoreKey, member);
                    if (payload == null) {
                        jedis.zrem(unackQueueName, member);
                        continue;
                    }

                    jedis.zadd(myQueueShard, score, member);
                    jedis.zrem(unackQueueName, member);
                }
                return null;
            });

        } finally {
            jedis.close();
            sw.stop();
        }

    }

    private AtomicInteger nextShardIndex = new AtomicInteger(0);

    private String getNextShard() {
        int indx = nextShardIndex.incrementAndGet();
        if (indx >= allShards.size()) {
            nextShardIndex.set(0);
            indx = 0;
        }
        String s = allShards.get(indx);
        return s;
    }

    private String getQueueShardKey(String queueName, String shard) {
        return redisKeyPrefix + ".QUEUE." + queueName + "." + shard;
    }

    private String getUnackKey(String queueName, String shard) {
        return redisKeyPrefix + ".UNACK." + queueName + "." + shard;
    }

    private <R> R execute(Callable<R> r) {
        return executeWithRetry(r, 0);
    }

    private <R> R executeWithRetry(Callable<R> r, int retryCount) {

        try {

            return r.call();

        } catch (ExecutionException e) {

            if (e.getCause() instanceof DynoException) {
                if (retryCount < this.retryCount) {
                    return executeWithRetry(r, ++retryCount);
                }
            }
            throw new RuntimeException(e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        schedulerForUnacksProcessing.shutdown();
        schedulerForPrefetchProcessing.shutdown();
        monitor.close();
    }

}

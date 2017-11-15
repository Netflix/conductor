package com.netflix.conductor.dao.queue;


import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

/**
 * @author Viren
 *
 * Please note that you should take care for disposing resource related to RedisQueue instances - that means you
 * should call close() on RedisQueue instance.
 */
public class RedisQueues implements Closeable {

    private JedisCommands quorumConn;

    private JedisCommands nonQuorumConn;

    private Set<String> allShards;

    private String shardName;

    private String redisKeyPrefix;

    private int unackTime;

    private int unackHandlerIntervalInMS;

    private JedisPool pool;

    private ConcurrentHashMap<String, Queue> queues;

    /**
     *
     * @param quorumConn Dyno connection with dc_quorum enabled
     * @param nonQuorumConn	Dyno connection to local Redis
     * @param redisKeyPrefix	prefix applied to the Redis keys
     * @param unackTime	Time in millisecond within which a message needs to be acknowledged by the client, after which the message is re-queued.
     * @param unackHandlerIntervalInMS	Time in millisecond at which the un-acknowledgement processor runs
     */
    public RedisQueues(JedisCommands quorumConn, JedisCommands nonQuorumConn, String redisKeyPrefix, int unackTime,
                       int unackHandlerIntervalInMS) {

        this.quorumConn = quorumConn;
        this.nonQuorumConn = nonQuorumConn;
        this.redisKeyPrefix = redisKeyPrefix;
        this.unackTime = unackTime;
        this.unackHandlerIntervalInMS = unackHandlerIntervalInMS;
        this.queues = new ConcurrentHashMap<>();
    }

    /**
     *
     * @param queueName Name of the queue
     * @return Returns the DynoQueue hosting the given queue by name
     */
    public Queue get(String queueName) {

        String key = queueName.intern();
        Queue queue = this.queues.get(key);
        if (queue != null) {
            return queue;
        }

        synchronized (this) {
//            queue = new RedisDynoQueue(redisKeyPrefix, queueName, allShards, shardName, unackHandlerIntervalInMS)
//                    .withUnackTime(unackTime)
//                    .withNonQuorumConn(nonQuorumConn)
//                    .withQuorumConn(quorumConn);
            queue = new RedisQueue(redisKeyPrefix, queueName, shardName, unackTime, pool);
            this.queues.put(key, queue);
        }

        return queue;
    }

    /**
     *
     * @return Collection of all the registered queues
     */
    public Collection<Queue> queues(){
        return this.queues.values();
    }

    @Override
    public void close() throws IOException {
        queues.values().forEach(queue -> {
            try {
                queue.close();
            }
            catch (final IOException e) {
                throw new RuntimeException(e.getCause());
            }
        });
    }
}
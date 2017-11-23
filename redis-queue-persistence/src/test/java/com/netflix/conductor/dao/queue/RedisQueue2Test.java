package com.netflix.conductor.dao.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisQueue2Test {

    private static Jedis dynoClient;

    private static final String queueName = "test_queue";

    private static final String redisKeyPrefix = "testdynoqueues";

    private static RedisQueue rdq;

    private static String messageKeyPrefix;

    private static int maxHashBuckets = 1024;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);
        config.setTestOnCreate(true);
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(60_000);
        JedisPool pool = new JedisPool(config, "localhost", 6379);
        dynoClient = new Jedis("localhost", 6379, 0, 0);
        dynoClient.flushAll();
        rdq = new RedisQueue(redisKeyPrefix, queueName, "x", 1_000, pool);
        messageKeyPrefix = redisKeyPrefix + ".MESSAGE.";
    }

    @Test
    public void testGetName() {
        assertEquals(queueName, rdq.getName());
    }

    @Test
    public void testGetUnackTime() {
        assertEquals(1_000, rdq.getUnackTime());
    }

    @Test
    public void testTimeoutUpdate() {

        rdq.clear();

        String id = UUID.randomUUID().toString();
        Message msg = new Message(id, "Hello World-" + id);
        msg.setTimeout(100, TimeUnit.MILLISECONDS);
        rdq.push(Arrays.asList(msg));

        List<Message> popped = rdq.pop(1, 10, TimeUnit.MILLISECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());

        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertNotNull(popped);
        assertEquals(1, popped.size());

        boolean updated = rdq.setUnackTimeout(id, 500);
        assertTrue(updated);
        popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());

        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        rdq.processUnacks();
        popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertNotNull(popped);
        assertEquals(1, popped.size());

        updated = rdq.setUnackTimeout(id, 10_000);	//10 seconds!
        assertTrue(updated);
        rdq.processUnacks();
        popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());

        updated = rdq.setUnackTimeout(id, 0);
        assertTrue(updated);
        rdq.processUnacks();
        popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertNotNull(popped);
        assertEquals(1, popped.size());

        rdq.ack(id);
        Map<String, Map<String, Long>> size = rdq.shardSizes();
        Map<String, Long> values = size.get("x");
        long total = values.values().stream().mapToLong(v -> v).sum();
        assertEquals(0, total);

        popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertNotNull(popped);
        assertEquals(0, popped.size());
    }

    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException {

        rdq.clear();

        final int count = 100;
        final AtomicInteger published = new AtomicInteger(0);

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(6);
        CountDownLatch publishLatch = new CountDownLatch(1);
        Runnable publisher = new Runnable() {

            @Override
            public void run() {
                List<Message> messages = new LinkedList<>();
                for (int i = 0; i < 10; i++) {
                    Message msg = new Message(UUID.randomUUID().toString(), "Hello World-" + i);
                    msg.setPriority(new Random().nextInt(98));
                    messages.add(msg);
                }
                if(published.get() >= count) {
                    publishLatch.countDown();
                    return;
                }

                published.addAndGet(messages.size());
                rdq.push(messages);
            }
        };

        for(int p = 0; p < 3; p++) {
            ses.scheduleWithFixedDelay(publisher, 1, 1, TimeUnit.MILLISECONDS);
        }
        publishLatch.await();
        CountDownLatch latch = new CountDownLatch(count);
        List<Message> allMsgs = new CopyOnWriteArrayList<>();
        AtomicInteger consumed = new AtomicInteger(0);
        AtomicInteger counter = new AtomicInteger(0);
        Runnable consumer = new Runnable() {

            @Override
            public void run() {
                if(consumed.get() >= count) {
                    return;
                }
                List<Message> popped = rdq.pop(100, 1, TimeUnit.MILLISECONDS);
                allMsgs.addAll(popped);
                consumed.addAndGet(popped.size());
                popped.stream().forEach(p -> latch.countDown());
                counter.incrementAndGet();
            }
        };
        for(int c = 0; c < 2; c++) {
            ses.scheduleWithFixedDelay(consumer, 1, 10, TimeUnit.MILLISECONDS);
        }
        Uninterruptibles.awaitUninterruptibly(latch);
        System.out.println("Consumed: " + consumed.get() + ", all: " + allMsgs.size() + " counter: " + counter.get());
        Set<Message> uniqueMessages = allMsgs.stream().collect(Collectors.toSet());

        assertEquals(count, allMsgs.size());
        assertEquals(count, uniqueMessages.size());
        List<Message> more = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertEquals(0, more.size());

        ses.shutdownNow();
    }

    @Test
    public void testSetTimeout() {

        rdq.clear();

        Message msg = new Message("x001yx", "Hello World");
        msg.setPriority(3);
        msg.setTimeout(10_000);
        rdq.push(Arrays.asList(msg));

        List<Message> popped = rdq.pop(1, 1, TimeUnit.SECONDS);
        assertTrue(popped.isEmpty());

        boolean updated = rdq.setTimeout(msg.getId(), 0);
        assertTrue(updated);
        popped = rdq.pop(2, 1, TimeUnit.SECONDS);
        assertEquals(1, popped.size());
        assertEquals(0, popped.get(0).getTimeout());
    }

    @Test
    public void testAll() {

        rdq.clear();
        assertEquals(0, rdq.size());

        int count = 10;
        List<Message> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            Message msg = new Message("" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }
        rdq.push(messages);

        messages = rdq.peek(count);

        assertNotNull(messages);
        assertEquals(count, messages.size());
        long size = rdq.size();
        assertEquals(count, size);

        // We did a peek - let's ensure the messages are still around!
        List<Message> messages2 = rdq.peek(count);
        assertNotNull(messages2);
        assertEquals(messages, messages2);

        List<Message> poped = rdq.pop(count, 1, TimeUnit.SECONDS);
        assertNotNull(poped);
        assertEquals(count, poped.size());
        assertEquals(messages, poped);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        rdq.processUnacks();

        for (Message msg : messages) {
            Message found = rdq.get(msg.getId());
            assertNotNull(found);
            assertEquals(msg.getId(), found.getId());
            assertEquals(msg.getTimeout(), found.getTimeout());
        }
        assertNull(rdq.get("some fake id"));

        List<Message> messages3 = rdq.pop(count, 1, TimeUnit.SECONDS);
        if(messages3.size() < count){
            List<Message> messages4 = rdq.pop(count, 1, TimeUnit.SECONDS);
            messages3.addAll(messages4);
        }

        assertNotNull(messages3);
        assertEquals(10, messages3.size());
        assertEquals(messages.stream().map(msg -> msg.getId()).sorted().collect(Collectors.toList()), messages3.stream().map(msg -> msg.getId()).sorted().collect(Collectors.toList()));
        assertEquals(10, messages3.stream().map(msg -> msg.getId()).collect(Collectors.toSet()).size());
        messages3.stream().forEach(System.out::println);
        int bucketCounts = 0;
        for(int i = 0; i < maxHashBuckets; i++) {
            bucketCounts += dynoClient.hlen(messageKeyPrefix + i + "." + queueName);
        }
        assertEquals(10, bucketCounts);

        for (Message msg : messages3) {
            assertTrue(rdq.ack(msg.getId()));
            assertFalse(rdq.ack(msg.getId()));
        }
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        messages3 = rdq.pop(count, 1, TimeUnit.SECONDS);
        assertNotNull(messages3);
        assertEquals(0, messages3.size());
    }

    @Before
    public void clear(){
        rdq.clear();
        int bucketCounts = 0;
        for(int i = 0; i < maxHashBuckets; i++) {
            bucketCounts += dynoClient.hlen(messageKeyPrefix + i + "." + queueName);
        }
        assertEquals(0, bucketCounts);
    }

    @Test
    public void testClearQueues() {
        rdq.clear();
        int count = 10;
        List<Message> messages = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            Message msg = new Message("x" + i, "Hello World-" + i);
            msg.setPriority(count - i);
            messages.add(msg);
        }

        rdq.push(messages);
        assertEquals(count, rdq.size());
        rdq.clear();
        assertEquals(0, rdq.size());

    }

}
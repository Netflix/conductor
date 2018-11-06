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
/**
 *
 */
package com.netflix.conductor.contribs.queue.shotgun;

import com.bydeluxe.onemq.OneMQ;
import com.bydeluxe.onemq.OneMQClient;
import com.bydeluxe.onemq.Subscription;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import d3sw.shotgun.shotgunpb.ShotgunOuterClass;
import io.nats.client.NUID;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Oleksiy Lysak
 */
public class ShotgunQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(ShotgunQueue.class);
    protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private AtomicReference<OneMQClient> conn = new AtomicReference<>();
    private AtomicReference<Subscription> subs = new AtomicReference<>();
    private AtomicBoolean listened = new AtomicBoolean();
    private AtomicBoolean closed = new AtomicBoolean(true);
    private ScheduledExecutorService execs;
    private int[] publishRetryIn;
    final String queueURI;
    final String service;
    final String subject;
    final String groupId;
    final String dns;

    public ShotgunQueue(String dns, String service, String queueURI, int[] publishRetryIn) {
        this.dns = dns;
        this.service = service;
        this.queueURI = queueURI;
        this.publishRetryIn = publishRetryIn;

        // If groupId specified (e.g. subject:groupId) - split to subject & groupId
        if (queueURI.contains(":")) {
            this.subject = queueURI.substring(0, queueURI.indexOf(':'));
            this.groupId = queueURI.substring(queueURI.indexOf(':') + 1);
        } else {
            this.subject = queueURI;
            this.groupId = null;
        }
        logger.debug(String.format("Initialized with queueURI=%s, subject=%s, groupId=%s", queueURI, subject, groupId));
        open();
    }

    @Override
    public Observable<Message> observe() {
        logger.debug("Observe invoked for queueURI " + queueURI);
        listened.set(true);

        subscribe();

        Observable.OnSubscribe<Message> onSubscribe = subscriber -> {
            Observable<Long> interval = Observable.interval(100, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x) -> {
                List<Message> available = new LinkedList<>();
                messages.drainTo(available);

                if (!available.isEmpty()) {
                    AtomicInteger count = new AtomicInteger(0);
                    StringBuilder buffer = new StringBuilder();
                    available.forEach(msg -> {
                        buffer.append(msg.getId()).append("=").append(msg.getPayload());
                        count.incrementAndGet();

                        if (count.get() < available.size()) {
                            buffer.append(",");
                        }
                    });

                    logger.debug(String.format("Batch from %s to conductor is %s", subject, buffer.toString()));
                }

                return Observable.from(available);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
        return Observable.create(onSubscribe);
    }

    @Override
    public String getType() {
        return EventQueues.QueueType.shotgun.name();
    }

    @Override
    public String getName() {
        return queueURI;
    }

    @Override
    public String getURI() {
        return queueURI;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        return Collections.emptyList();
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return messages.size();
    }

    @Override
    public void publish(List<Message> messages) {
        messages.forEach(message -> {
            String payload = message.getPayload();
            try {
                logger.info(String.format("Trying to publish to %s: %s", subject, payload));
                publishWait(subject, payload);
                logger.info(String.format("Published to %s: %s", subject, payload));
            } catch (Exception eo) {
                logger.error(String.format("Failed to publish to %s: %s", subject, payload), eo);
                closeConn();
                if (ArrayUtils.isEmpty(publishRetryIn)) {
                    throw new RuntimeException(eo);
                }
                for (int i = 0; i < publishRetryIn.length; i++) {
                    try {
                        int delay = publishRetryIn[i];
                        logger.error(String.format("Publish retry in %s seconds for %s", delay, subject));

                        Thread.sleep(delay * 1000L);

                        logger.info(String.format("Trying to publish to %s: %s", subject, payload));
                        publishWait(subject, payload);
                        logger.info(String.format("Published to %s: %s", subject, payload));

                        break;
                    } catch (Exception ex) {
                        logger.error(String.format("Failed to publish to %s: %s", subject, payload), ex);
                        closeConn();
                        // Latest attempt
                        if (i == (publishRetryIn.length - 1)) {
                            logger.error(String.format("Publish gave up for %s: %s", subject, payload));
                            throw new RuntimeException(ex.getMessage(), ex);
                        }
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        logger.debug("Closing connection for " + queueURI);
        if (execs != null) {
            execs.shutdownNow();
            execs = null;
        }
        closeConn();
        closed.set(true);
    }

    public void open() {
        // do nothing if not closed
        if (!closed.get()) {
            return;
        }

        try {
            connect();

            // Re-initiated subscription if existed
            if (listened.get()) {
                subscribe();
            }
        } catch (Exception ignore) {
        }

        execs = Executors.newScheduledThreadPool(1);
        execs.scheduleAtFixedRate(this::monitor, 0, 500, TimeUnit.MILLISECONDS);
        closed.set(false);
    }

    private void monitor() {
        try {
            boolean observed = listened.get();
            boolean connected = isConnected();
            boolean subscribed = isSubscribed();

            // All conditions are met
            if (connected && observed && subscribed) {
                return;
            } else if (connected && !observed && !subscribed) {
                return;
            }

            logger.error("Monitor invoked for " + queueURI);
            closeConn();

            // Connect
            connect();

            // Re-initiated subscription if existed
            if (observed) {
                subscribe();
            }
        } catch (Exception ex) {
            logger.error("Monitor failed with " + ex.getMessage() + " for " + queueURI, ex);
        }
    }

    private void publishWait(String subject, String payload) throws Exception {
        LinkedBlockingDeque<Object> queue = new LinkedBlockingDeque<>();

        Runnable task = () -> {
            try {
                publish(subject, payload.getBytes());
                queue.add(Boolean.TRUE);
            } catch (Exception ex) {
                queue.add(ex);
            }
        };

        Thread thread = new Thread(task);
        thread.start();

        // Publish must be really fast and no need to wait longer than even 3 seconds
        Object result = queue.poll(3, TimeUnit.SECONDS);
        if (result == null) {
            thread.interrupt();
            throw new RuntimeException("Publish timed out");
        } else if (result instanceof Exception) {
            Exception ex = (Exception) result;
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    private void ensureConnected() {
        if (!isConnected()) {
            throw new RuntimeException("No OneMQ connection");
        }
    }

    private void connect() {
        try {
            OneMQClient temp = new OneMQ();
            temp.connect(dns, null, null);
            logger.debug("Successfully connected for " + queueURI);

            conn.set(temp);
        } catch (Exception e) {
            logger.error("Unable to establish shotgun connection for " + queueURI, e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private boolean isConnected() {
        return (conn.get() != null && !conn.get().isConnected());
    }

    private boolean isSubscribed() {
        return subs.get() != null;

    }

    private void publish(String subject, byte[] data) throws Exception {
        ensureConnected();
        conn.get().publish(subject, data);
    }

    private void subscribe() {
        if (isSubscribed()) {
            return;
        }

        try {
            ensureConnected();

            Subscription tmpSubs;

            // Create subject/groupId subscription if the groupId has been provided
            if (StringUtils.isNotEmpty(groupId)) {
                logger.debug("No subscription. Creating subscription with subject={}, groupId={}", subject, groupId);
                tmpSubs = conn.get().subscribe(subject, service, groupId, this::onMessage);
            } else {
                String uuid = UUID.randomUUID().toString();
                logger.debug("No subscription. Creating subscription with subject={}, groupId={}", subject, uuid);
                tmpSubs = conn.get().subscribe(subject, service, uuid, this::onMessage);
            }

            subs.set(tmpSubs);
        } catch (Exception ex) {
            logger.error("Subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
        }
    }

    private void onMessage(Subscription subscription, ShotgunOuterClass.Message message) {
        String payload = message.getContent().toStringUtf8();
        logger.debug(String.format("Received message for %s: %s", subscription.getSubject(), payload));

        Message dstMsg = new Message();
        dstMsg.setId(NUID.nextGlobal());
        dstMsg.setPayload(payload);

        messages.add(dstMsg);
        Monitors.recordEventQueueMessagesReceived(EventQueues.QueueType.shotgun.name(), queueURI);
    }

    private void closeConn() {
        if (subs.get() != null) {
            try {
                conn.get().unsubscribe(subs.get());
            } catch (Exception ignore) {
            }
            subs.set(null);
        }
        if (conn.get() != null) {
            try {
                conn.get().close();
            } catch (Exception ignore) {
            }
            conn.set(null);
        }
    }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Oleksiy Lysak
 */
public class ShotgunQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(ShotgunQueue.class);
    protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private ScheduledExecutorService execs;
    private Duration[] publishRetryIn;
    private final String queueURI;
    private final String service;
    private final String subject;
    private final String groupId;
    private final String dns;
    private Subscription subs;
    private OneMQClient conn;

    public ShotgunQueue(String dns, String service, String queueURI, Duration[] publishRetryIn) {
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

        conn = new OneMQ();
        execs = Executors.newScheduledThreadPool(1);
        execs.scheduleAtFixedRate(this::monitor, 0, 500, TimeUnit.MILLISECONDS);
    }

    private void monitor() {
        if (conn.isConnected()) {
            return;
        }
        try {
            conn.close();
            conn.connect(dns, null, null);
        } catch (Exception ex) {
            logger.error("OneMQ client connect failed {}", ex.getMessage(), ex);
        }
    }

    @Override
    public Observable<Message> observe() {
        logger.debug("Observe invoked for queueURI " + queueURI);

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
                logger.debug(String.format("Trying to publish to %s: %s", subject, payload));
                conn.publish(subject, payload.getBytes(), publishRetryIn);
                logger.info(String.format("Published to %s: %s", subject, payload));
            } catch (Exception eo) {
                logger.error(String.format("Failed to publish to %s: %s", subject, payload), eo);
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
        if (subs != null) {
            try {
                conn.unsubscribe(subs);
            } catch (Exception ignore) {
            }
            subs = null;
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignore) {
            }
            conn = null;
        }
    }

    private void subscribe() {
        if (subs != null) {
            return;
        }

        try {
            // Create subject/groupId subscription if the groupId has been provided
            if (StringUtils.isNotEmpty(groupId)) {
                logger.debug("Creating subscription with subject={}, groupId={}", subject, groupId);
                subs = conn.subscribe(subject, service, groupId, this::onMessage);
            } else {
                String uuid = UUID.randomUUID().toString();
                logger.debug("Creating subscription with subject={}, groupId={}", subject, uuid);
                subs = conn.subscribe(subject, service, uuid, this::onMessage);
            }
        } catch (Exception ex) {
            logger.error("Subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
        }
    }

    private void onMessage(Subscription subscription, ShotgunOuterClass.Message message) {
        String payload = message.getContent().toStringUtf8();
        logger.info(String.format("Received message for %s: %s", subscription.getSubject(), payload));

        Message dstMsg = new Message();
        dstMsg.setId(NUID.nextGlobal());
        dstMsg.setPayload(payload);

        messages.add(dstMsg);
        Monitors.recordEventQueueMessagesReceived(EventQueues.QueueType.shotgun.name(), queueURI);
    }
}

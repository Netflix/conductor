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

import com.bydeluxe.onemq.OneMQClient;
import com.bydeluxe.onemq.Subscription;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import d3sw.shotgun.shotgunpb.ShotgunOuterClass;
import io.nats.client.NUID;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 */
public class SharedShotgunQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(SharedShotgunQueue.class);
    protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private Duration[] publishRetryIn;
    private boolean manualAck;
    private final String queueURI;
    private final String service;
    private final String subject;
    private final String groupId;
    private OneMQClient conn;
    private Subscription subs;

    public SharedShotgunQueue(OneMQClient conn, String service, String queueURI, Duration[] publishRetryIn, boolean manualAck) {
        this.conn = conn;
        this.service = service;
        this.queueURI = queueURI;
        this.publishRetryIn = publishRetryIn;
        this.manualAck = manualAck;

        // If groupId specified (e.g. subject:groupId) - split to subject & groupId
        if (queueURI.contains(":")) {
            this.subject = queueURI.substring(0, queueURI.indexOf(':'));
            this.groupId = queueURI.substring(queueURI.indexOf(':') + 1);
        } else {
            this.subject = queueURI;
            this.groupId = null;
        }
        logger.debug(String.format("Initialized with queueURI=%s, subject=%s, groupId=%s", queueURI, subject, groupId));
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
        if (!manualAck) {
            return Collections.emptyList();
        }
        messages.forEach(msg -> {
            try {
                conn.ack(msg.getReceipt());
            } catch (Exception e) {
                logger.error("ack failed with " + e.getMessage() + " for " + msg.getId(), e);
            }
        });
        return Collections.emptyList();
    }

    @Override
    public void unack(List<Message> messages) {
        messages.forEach(msg -> {
            try {
                conn.unack(msg.getReceipt());
            } catch (Exception e) {
                logger.error("unack failed with " + e.getMessage() + " for " + msg.getId(), e);
            }
        });
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
                conn.publish(subject, payload.getBytes(), message.getTraceId(), publishRetryIn);
                logger.info(String.format("Published to %s: %s", subject, payload));
            } catch (Exception eo) {
                logger.error(String.format("Failed to publish to %s: %s", subject, payload), eo);
            }
        });
    }

    @Override
    public void close() {
        logger.debug("Closing connection for " + queueURI);
        if (subs != null) {
            try {
                conn.unsubscribe(subs);
            } catch (Exception ignore) {
            }
            subs = null;
        }
    }

    private void subscribe() {
        if (subs != null) {
            return;
        }

        try {
            // Create subject/groupId subscription if the groupId has been provided
            if (StringUtils.isNotEmpty(groupId)) {
                logger.debug("No subscription. Creating subscription with subject={}, groupId={}", subject, groupId);
                subs = conn.subscribe(subject, service, groupId, this::onMessage);
                subs.setManualAck(manualAck);
            } else {
                String uuid = UUID.randomUUID().toString();
                logger.debug("No subscription. Creating subscription with subject={}, groupId={}", subject, uuid);
                subs = conn.subscribe(subject, service, uuid, this::onMessage);
                subs.setManualAck(manualAck);
            }
        } catch (Exception ex) {
            logger.error("Subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
        }
    }

    private void onMessage(Subscription subscription, ShotgunOuterClass.Message message) {
        String payload = message.getContent().toStringUtf8();

        Message dstMsg = new Message();
        dstMsg.setId(NUID.nextGlobal());
        dstMsg.setReceipt(message.getID());
        dstMsg.setPayload(payload);
        dstMsg.setReceived(System.currentTimeMillis());
        dstMsg.setTraceId(message.getTraceID());

        NDC.push("event-"+dstMsg.getId());
        try {
            logger.info(String.format("Received message for %s %s=%s", subscription.getSubject(), dstMsg.getId(), payload));
        } finally {
            NDC.remove();
        }

        messages.add(dstMsg);
        Monitors.recordEventQueueMessagesReceived(EventQueues.QueueType.shotgun.name(), queueURI);
    }
}

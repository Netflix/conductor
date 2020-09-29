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
import com.bydeluxe.onemq.PublishOptions;
import com.bydeluxe.onemq.Subscription;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.events.queue.OnMessageHandler;
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
    protected Logger logger = LoggerFactory.getLogger(getClass());
    private Duration[] publishRetryIn;
    protected OnMessageHandler handler;
    protected final String service;
    protected final String subject;
    private final String groupId;
    private boolean manualAck;
    private int prefetchSize;
    final String queueURI;
    OneMQClient conn;
    Subscription subs;

    public SharedShotgunQueue(OneMQClient conn, String service, String queueURI, Duration[] publishRetryIn,
                              boolean manualAck, int prefetchSize, OnMessageHandler handler) {
        this.conn = conn;
        this.service = service;
        this.queueURI = queueURI;
        this.publishRetryIn = publishRetryIn;
        this.manualAck = manualAck;
        this.prefetchSize = prefetchSize;
        this.handler = handler;

        // If groupId specified (e.g. subject:groupId) - split to subject & groupId
        if (queueURI.contains(":")) {
            this.subject = queueURI.substring(0, queueURI.indexOf(':'));
            this.groupId = queueURI.substring(queueURI.indexOf(':') + 1);
        } else {
            this.subject = queueURI;
            this.groupId = UUID.randomUUID().toString();
        }

        logger.debug(String.format("Init queueURI=%s, subject=%s, groupId=%s, manualAck=%s, prefetchSize=%s",
            queueURI, subject, groupId, manualAck, prefetchSize));
    }

    @Override
    public Observable<Message> observe() {
        if (subs != null) {
            return null;
        }

        try {
            logger.debug(String.format("Start subscription subject=%s, groupId=%s, manualAck=%s, prefetchSize=%s",
                subject, groupId, manualAck, prefetchSize));
            subs = conn.subscribe(subject, service, groupId, manualAck, prefetchSize, this::onMessage);
        } catch (Exception ex) {
            logger.debug("Subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
        }

        return null;
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
                logger.debug("ack failed with " + e.getMessage() + " for " + msg.getId(), e);
            }
        });
        return Collections.emptyList();
    }

    @Override
    public void unack(List<Message> messages) {
        messages.forEach(msg -> unack(msg.getReceipt()));
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public int getPrefetchSize() {
        return prefetchSize;
    }

    @Override
    public void publish(List<Message> messages) {
        messages.forEach(message -> {
            String payload = message.getPayload();
            try {
                PublishOptions options = PublishOptions.newBuilder()
                    .withHeaders(message.getHeaders())
                    .withTraceId(message.getTraceId())
                    .withDelays(publishRetryIn)
                    .withClientId(service)
                    .build();

                logger.debug(String.format("Publishing to %s: %s", subject, payload));
                conn.publishWithOptions(subject, payload.getBytes(), options);
                logger.info(String.format("Published to %s: %s", subject, payload));
            } catch (Exception eo) {
                logger.debug(String.format("Publish failed for %s: %s", subject, payload), eo);
            }
        });
    }

    @Override
    public void close() {
        if (subs != null) {
            try {
                conn.unsubscribe(subs);
            } catch (Exception ignore) {
            }
            subs = null;
        }
        logger.debug("Closed for " + queueURI);
    }

    private void unack(String msgId) {
        if (!manualAck) {
            return;
        }
        try {
            conn.unack(msgId);
        } catch (Exception e) {
            logger.debug("unack failed with " + e.getMessage() + " for " + msgId, e);
        }
    }

    private void onMessage(Subscription subscription, ShotgunOuterClass.Message message) {
        String uuid = UUID.randomUUID().toString();
        NDC.push("event-" + uuid);
        try {
            String payload = message.getContent().toStringUtf8();

            Message dstMsg = new Message();
            dstMsg.setId(uuid);
            dstMsg.setReceipt(message.getID());
            dstMsg.setPayload(payload);
            dstMsg.setReceived(System.currentTimeMillis());
            dstMsg.setTraceId(message.getTraceID());

            logger.info(String.format("ShotgunMsg: Received message for %s/%s/%s %s=%s",
                subscription.getSubject(), subscription.getGroupID(), message.getTraceID(), dstMsg.getId(), payload));

            if (handler != null) {
                handler.apply(this, dstMsg);
            } else {
                ack(Collections.singletonList(dstMsg));
                logger.debug("No handler - ack " + dstMsg.getReceipt());
                logger.info("ShotgunMsg:q: ACKing Message - No Handler " + dstMsg.getReceipt());
            }
        } catch (Exception ex) {
            logger.debug("onMessage failed " + ex.getMessage(), ex);
            logger.info("ShotgunMsg:q: UNACK Message.. Exception" + ex.getMessage(), ex);
            unack(message.getID());
        } finally {
            NDC.remove();
        }
    }
}

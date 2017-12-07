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
package com.netflix.conductor.contribs.queue.nats;

import com.netflix.conductor.core.events.EventQueues;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 *
 */
public class NATSStreamObservableQueue extends NATSAbstractQueue {
    private static Logger logger = LoggerFactory.getLogger(NATSStreamObservableQueue.class);
    private StreamingConnectionFactory fact;
    private StreamingConnection conn;
    private Subscription subs;
    private String durableName;

    public NATSStreamObservableQueue(StreamingConnectionFactory factory, String queueURI, String durableName) {
        super(queueURI, EventQueues.QueueType.nats_stream);
        this.fact = factory;
        this.durableName = durableName;

        // Init instance regardless of the exception it might throw
        try {
            this.conn = openConnection();

            // Start maintenance with delay
            Executors.newScheduledThreadPool(1)
                    .scheduleAtFixedRate(this::maintain, 30_000, 500, TimeUnit.MILLISECONDS);
        } catch (Exception ignore) {
            // Start maintenance immediately
            Executors.newScheduledThreadPool(1)
                    .scheduleAtFixedRate(this::maintain, 0, 500, TimeUnit.MILLISECONDS);
        }
    }

    private StreamingConnection openConnection() {
        try {
            StreamingConnection temp = fact.createConnection();
            logger.info("Successfully connected for " + queueURI);

            temp.getNatsConnection().setReconnectedCallback((event) ->
                    logger.warn("onReconnect. Reconnected back for " + queueURI));
            temp.getNatsConnection().setDisconnectedCallback((event ->
                    logger.warn("onDisconnect. Disconnected for " + queueURI)));

            return temp;
        } catch (Exception e) {
            logger.error("Unable to establish nats streaming connection for " + queueURI, e);
            throw new RuntimeException(e);
        }
    }

    private void maintain() {
        if (conn != null && conn.getNatsConnection().isConnected()) {
            return;
        }
        logger.error("Maintenance invoked for " + queueURI);
        mu.lock();
        try {
            if (subs != null) {
                subs.close();
                subs = null;
            }

            if (conn != null) {
                conn.close();
                conn = null;
            }
            // Connect
            conn = openConnection();

            // Re-initiated subscription if existed
            if (observable) {
                subs = null;
                subscribe();
            }
        } catch (Exception ex) {
            logger.error("Maintenance failed with " + ex.getMessage() + " for " + queueURI, ex);
        } finally {
            mu.unlock();
        }
    }

    private void ensureConnected() {
        if (conn == null || !conn.getNatsConnection().isConnected()) {
            throw new RuntimeException("No nats streaming connection");
        }
    }

    void subscribe() {
        // do nothing if already subscribed
        if (subs != null) {
            return;
        }

        try {
            SubscriptionOptions subscriptionOptions = new SubscriptionOptions
                    .Builder().durableName(durableName).build();

            // Create subject/queue subscription if the queue has been provided
            if (StringUtils.isNotEmpty(queue)) {
                logger.info("No subscription. Creating a queue subscription. subject={}, queue={}", subject, queue);
                subs = conn.subscribe(subject, queue,
                        natMsg -> onMessage(subject, natMsg.getData()), subscriptionOptions);
            } else {
                logger.info("No subscription. Creating a pub/sub subscription. subject={}", subject);
                subs = conn.subscribe(subject,
                        natMsg -> onMessage(subject, natMsg.getData()), subscriptionOptions);
            }
        } catch (Exception e) {
            String error = "Unable to start subscription for queueURI=" + queueURI;
            logger.error(error, e);
            throw new RuntimeException(error);
        }
    }

    @Override
    public void publish(String subject, byte[] data) throws Exception {
        ensureConnected();
        conn.publish(subject, data);
    }
}

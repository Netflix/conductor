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
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Subscription;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 *
 */
public class NATSObservableQueue extends NATSAbstractQueue {
    private static Logger logger = LoggerFactory.getLogger(NATSObservableQueue.class);
    private ConnectionFactory fact;
    private Subscription subs;
    private Connection conn;

    public NATSObservableQueue(ConnectionFactory factory, String queueURI) {
        super(queueURI, EventQueues.QueueType.nats);
        this.fact = factory;

        try {
            this.conn = openConnection();
        } catch (Exception ignore) {
        }

        // Start monitor
        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(this::monitor, 0, 500, TimeUnit.MILLISECONDS);
    }

    private Connection openConnection() {
        try {
            Connection temp = fact.createConnection();
            logger.info("Successfully connected for " + queueURI);

            temp.setReconnectedCallback((event) -> logger.warn("onReconnect. Reconnected back for " + queueURI));
            temp.setDisconnectedCallback((event -> logger.warn("onDisconnect. Disconnected for " + queueURI)));

            return temp;
        } catch (Exception e) {
            logger.error("Unable to establish nats connection for " + queueURI, e);
            throw new RuntimeException(e);
        }
    }

    private void monitor() {
        if (conn != null && conn.isConnected()) {
            return;
        }
        logger.error("Monitor invoked for " + queueURI);
        mu.lock();
        try {
            if (conn != null) {
                // That will also close all subscriptions assigned to this connection
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
            logger.error("Monitor failed with " + ex.getMessage() + " for " + queueURI, ex);
        } finally {
            mu.unlock();
        }
    }

    private void ensureConnected() {
        if (conn == null || !conn.isConnected()) {
            throw new RuntimeException("No nats connection");
        }
    }

    void subscribe() {
        // do nothing if already subscribed
        if (subs != null) {
            return;
        }

        try {
            ensureConnected();

            // Create subject/queue subscription if the queue has been provided
            if (StringUtils.isNotEmpty(queue)) {
                logger.info("No subscription. Creating a queue subscription. subject={}, queue={}", subject, queue);
                subs = conn.subscribe(subject, queue, msg -> onMessage(msg.getSubject(), msg.getData()));
            } else {
                logger.info("No subscription. Creating a pub/sub subscription. subject={}", subject);
                subs = conn.subscribe(subject, msg -> onMessage(msg.getSubject(), msg.getData()));
            }
        } catch (Throwable ex) {
            logger.error("Start subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
        }
    }

    @Override
    public void publish(String subject, byte[] data) throws Exception {
        ensureConnected();
        conn.publish(subject, data);
    }
}

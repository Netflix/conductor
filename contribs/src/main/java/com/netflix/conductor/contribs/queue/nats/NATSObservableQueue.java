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
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import io.nats.client.AsyncSubscription;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Oleksiy Lysak
 *
 */
public class NATSObservableQueue extends NATSAbstractQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(NATSObservableQueue.class);
    private final Lock mu = new ReentrantLock();
    private AsyncSubscription subs;
    private ConnectionFactory fact;
    private Connection conn;

    public NATSObservableQueue(ConnectionFactory factory, String queueURI) {
        super(queueURI);
        this.fact = factory;

        // Init connection regardless of the exception
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

    private Connection openConnection() {
        try {
            Connection conn = fact.createConnection();
            logger.info("Successfully connected for " + queueURI);

            conn.setReconnectedCallback((event) -> logger.warn("onReconnect. Reconnected back for " + queueURI));
            conn.setDisconnectedCallback((event -> logger.warn("onDisconnect. Disconnected for " + queueURI)));

            return conn;
        } catch (Exception e) {
            logger.error("Unable to establish nats connection for " + queueURI, e);
            throw new RuntimeException(e);
        }
    }

    private void maintain() {
        if (conn != null && conn.isConnected()) {
            return;
        }
        logger.error("Maintenance invoked for " + queueURI);
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
            logger.error("Maintenance failed with " + ex.getMessage() + " for " + queueURI, ex);
        }
    }

    private void ensureConnected() {
        if (conn == null || !conn.isConnected()) {
            throw new RuntimeException("No nats connection");
        }
    }

    private void subscribe() {
        // do nothing if already subscribed
        if (subs != null) {
            return;
        }

        try {
            ensureConnected();

            // Create subject/queue subscription if the queue has been provided
            if (StringUtils.isNotEmpty(queue)) {
                logger.info("No subscription. Creating a queue subscription. subject={}, queue={}", subject, queue);
                subs = conn.subscribe(subject, queue, natMsg -> onMessage(natMsg.getSubject(), natMsg.getData()));
            } else {
                logger.info("No subscription. Creating a pub/sub subscription. subject={}", subject);
                subs = conn.subscribe(subject, natMsg -> onMessage(natMsg.getSubject(), natMsg.getData()));
            }
        } catch (Exception ex) {
            logger.error("Start subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
        }
    }

    @Override
    public Observable<Message> observe() {
        logger.info("Observe invoked for queueURI " + queueURI);
        observable = true;

        subscribe();

        return getOnSubscribe();
    }

    @Override
    public String getType() {
        return EventQueues.QueueType.nats.name();
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
    public void publish(List<Message> messages) {
        ensureConnected();
        super.publish(messages);
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return messages.size();
    }

    @Override
    public void publish(String subject, byte[] data) throws Exception {
        conn.publish(subject, data);
    }
}

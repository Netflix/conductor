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
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collections;
import java.util.List;

/**
 * @author Oleksiy Lysak
 *
 */
public class NATSStreamObservableQueue extends NATSAbstractQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(NATSStreamObservableQueue.class);
    private StreamingConnectionFactory factory;
    private StreamingConnection connection;
    private Subscription subscription;
    private String durableName;

    public NATSStreamObservableQueue(StreamingConnectionFactory factory, String queueURI, String durableName) {
        super(queueURI);
        this.factory = factory;
        this.durableName = durableName;
        this.connection = openConnection();
    }

    private StreamingConnection openConnection() {
        try {
            return factory.createConnection();
        } catch (Exception e) {
            logger.error("Unable to establish NATS Streaming Connection for " + queueURI, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Observable<Message> observe() {
        logger.info("Observe invoked for queueURI " + queueURI);
        if (subscription == null) {
            try {
                SubscriptionOptions subscriptionOptions = new SubscriptionOptions
                        .Builder().durableName(durableName).build();

                // Create subject/queue subscription if the queue has been provided
                if (StringUtils.isNotEmpty(queue)) {
                    logger.info("No subscription. Creating a queue subscription. subject={}, queue={}", subject, queue);
                    subscription = connection.subscribe(subject, queue,
                            natMsg -> onMessage(subject, natMsg.getData()), subscriptionOptions);
                } else {
                    logger.info("No subscription. Creating a pub/sub subscription. subject={}", subject);
                    subscription = connection.subscribe(subject,
                            natMsg -> onMessage(subject, natMsg.getData()), subscriptionOptions);
                }
            } catch (Exception e) {
                String error = "Unable to start subscription for queueURI=" + queueURI;
                logger.error(error, e);
                throw new RuntimeException(error);
            }
        }

        return getOnSubscribe();
    }

    @Override
    public String getType() {
        return EventQueues.QueueType.nats_stream.name();
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
        if (!connection.getNatsConnection().isConnected()) {
            throw new RuntimeException("No nats_stream server connection");
        }
        super.publish(messages);
    }

    @Override
    public void publish(String subject, byte[] data) throws Exception {
        connection.publish(subject, data);
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return messages.size();
    }
}

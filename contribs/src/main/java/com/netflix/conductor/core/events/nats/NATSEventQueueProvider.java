/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.conductor.core.events.nats;

import com.netflix.conductor.contribs.queue.nats.NATSObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.EventQueues.QueueType;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
public class NATSEventQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(NATSEventQueueProvider.class);
    private Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();
    private ConnectionFactory connectionFactory;

    @Inject
    public NATSEventQueueProvider(Configuration config) {
        logger.info("NATS Event Queue Provider init");

        // Get NATS Streaming options
        String clusterId = config.getProperty("io.nats.streaming.clusterId", "test-cluster");
        String clientId = config.getProperty("io.nats.streaming.clientId", "test-client");
        String natsUrl = config.getProperty("io.nats.streaming.url", "nats://localhost:4222");

        logger.info("NATS Streaming clusterId=" + clusterId + ", clientId=" + clientId + ", natsUrl=" + natsUrl);

        // Init NATS Streaming API
        connectionFactory = new ConnectionFactory();
        connectionFactory.setClusterId(clusterId);
        connectionFactory.setClientId(clientId);
        connectionFactory.setNatsUrl(natsUrl);

        EventQueues.registerProvider(QueueType.nats, this);
        logger.info("NATS Event Queue Provider initialized...");
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> {
            Connection connection;
            try {
                connection = connectionFactory.createConnection();

                // Using queueURI as the subject and queue group.
                // All subscribers with the same queue name will form the queue group and only one member of the group
                // will be selected to receive any given message asynchronously.
                return new NATSObservableQueue(connection, queueURI, "conductor");
            } catch (Exception e) {
                logger.error("Unable to create connection for " + queueURI, e);
                throw new RuntimeException(e);
            }
        });
    }
}

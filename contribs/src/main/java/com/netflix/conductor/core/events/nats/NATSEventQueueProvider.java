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
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
public class NATSEventQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(NATSEventQueueProvider.class);
    protected Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();
    private Connection connection;

    @Inject
    public NATSEventQueueProvider(Configuration config) {
        logger.info("NATS Event Queue Provider init");

        // Init NATS API
        Properties props = new Properties();
        props.putAll(config.getAll());
        ConnectionFactory connectionFactory = new ConnectionFactory(props);
        try {
            connection = connectionFactory.createConnection();
        } catch (Exception e) {
            logger.error("Unable to create NATS Connection", e);
            throw new RuntimeException(e);
        }

        EventQueues.registerProvider(QueueType.nats, this);
        logger.info("NATS Event Queue Provider initialized...");
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> new NATSObservableQueue(connection, queueURI));
    }
}

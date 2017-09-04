/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.contribs;

import com.google.inject.AbstractModule;
import com.netflix.conductor.core.events.nats.NATSEventQueueProvider;
import io.nats.stan.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * @author Oleksiy Lysak
 *
 */
public class NatsModule extends AbstractModule {
    private static Logger logger = LoggerFactory.getLogger(NatsModule.class);

	@Override
	protected void configure() {
        logger.info("NATS Streaming Module configuration started ...");
        // Merge system env & properties into single map
        Properties props = new Properties();
        props.putAll(System.getenv());
        props.putAll(System.getProperties());

        // Get NATS Streaming options
        String clusterId = props.getProperty("io.nats.streaming.clusterId", "test-cluster");
        String clientId = props.getProperty("io.nats.streaming.clientId", "test-client");
        String natsUrl = props.getProperty("io.nats.streaming.url", "nats://localhost:4222");

        logger.info("NATS Streaming clusterId=" + clusterId + ", clientId=" + clientId + ", natsUrl=" + natsUrl);

        // Init NATS Streaming API
        ConnectionFactory factory = new ConnectionFactory();
        factory.setClusterId(clusterId);
        factory.setClientId(clientId);
        factory.setNatsUrl(natsUrl);
        bind(ConnectionFactory.class).toInstance(factory);
		bind(NATSEventQueueProvider.class).asEagerSingleton();

		logger.info("NATS Streaming Module configured ...");
	}

}

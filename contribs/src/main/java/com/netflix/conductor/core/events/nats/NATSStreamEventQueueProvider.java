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

import com.netflix.conductor.contribs.queue.nats.NATSStreamObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.EventQueues.QueueType;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import io.nats.client.Options;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * @author Oleksiy Lysak
 */
@Singleton
public class NATSStreamEventQueueProvider implements EventQueueProvider {
	private static Logger logger = LoggerFactory.getLogger(NATSStreamEventQueueProvider.class);
	protected Map<String, NATSStreamObservableQueue> queues = new ConcurrentHashMap<>();
	private String durableName;
	private String clusterId;
	private String natsUrl;
	private int[] publishRetryIn;

	@Inject
	public NATSStreamEventQueueProvider(Configuration config) {
		logger.info("NATS Stream Event Queue Provider init");

		// Get NATS Streaming options
		clusterId = config.getProperty("io.nats.streaming.clusterId", "test-cluster");
		durableName = config.getProperty("io.nats.streaming.durableName", null);
		natsUrl = config.getProperty("io.nats.streaming.url", Options.DEFAULT_URL);

		String[] arr = config.getProperty("io.nats.streaming.publishRetryIn", "5,10,15").split(",");
		publishRetryIn = Stream.of(arr).mapToInt(Integer::parseInt).toArray();

		logger.info("NATS Streaming clusterId=" + clusterId +
				", natsUrl=" + natsUrl + ", durableName=" + durableName + ", publishRetryIn=" + ArrayUtils.toString(publishRetryIn));

		EventQueues.registerProvider(QueueType.nats_stream, this);
		logger.info("NATS Stream Event Queue Provider initialized...");
	}

	@Override
	public ObservableQueue getQueue(String queueURI) {
		NATSStreamObservableQueue queue = queues.computeIfAbsent(queueURI, q ->
				new NATSStreamObservableQueue(clusterId, natsUrl, durableName, queueURI, publishRetryIn));
		if (queue.isClosed()) {
			queue.open();
		}
		return queue;
	}
}

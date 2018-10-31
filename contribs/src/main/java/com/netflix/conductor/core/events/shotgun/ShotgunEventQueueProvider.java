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
package com.netflix.conductor.core.events.shotgun;

import com.netflix.conductor.contribs.queue.shotgun.ShotgunQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.EventQueues.QueueType;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
public class ShotgunEventQueueProvider implements EventQueueProvider {
	private static Logger logger = LoggerFactory.getLogger(ShotgunEventQueueProvider.class);
	private static final String PROP_SERVICE = "io.shotgun.service";
	private static final String PROP_DNS = "io.shotgun.dns";
	protected Map<String, ShotgunQueue> queues = new ConcurrentHashMap<>();
	private int[] publishRetryIn;
	private String service;
	private String dns;

	@Inject
	public ShotgunEventQueueProvider(Configuration config) {
		logger.debug("Shotgun Event Queue Provider init");

		service = config.getProperty(PROP_SERVICE, null);
		if (StringUtils.isEmpty(service)) {
			throw new RuntimeException("No " + PROP_SERVICE + " property configured");
		}

		dns = config.getProperty(PROP_DNS, null);
		if (StringUtils.isEmpty(dns)) {
			throw new RuntimeException("No " + PROP_DNS + " property configured");
		}

		String[] arr = config.getProperty("io.shotgun.publishRetryIn", ",").split(",");
		publishRetryIn = Stream.of(arr).mapToInt(Integer::parseInt).toArray();

		logger.debug("Shotgun Event Queue Provider settings are dns=" + dns + ", service=" + service
				+ ", publishRetryIn=" + ArrayUtils.toString(publishRetryIn));

		EventQueues.registerProvider(QueueType.shotgun, this);
		logger.debug("Shotgun Event Queue Provider initialized...");
	}

	@Override
	public ObservableQueue getQueue(String queueURI) {
		ShotgunQueue queue = queues.computeIfAbsent(queueURI, q -> new ShotgunQueue(dns, service, queueURI, publishRetryIn));
		if (queue.isClosed()) {
			queue.open();
		}
		return queue;
	}
}

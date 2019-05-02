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

import com.bydeluxe.onemq.OneMQ;
import com.bydeluxe.onemq.OneMQClient;
import com.netflix.conductor.contribs.queue.shotgun.SharedShotgunQueue;
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
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 */
@Singleton
public class ShotgunEventQueueProvider implements EventQueueProvider {
	private static Logger logger = LoggerFactory.getLogger(ShotgunEventQueueProvider.class);
	private static final String PROP_MANUAL_ACK = "io.shotgun.manualAck";
	private static final String PROP_SHARED = "io.shotgun.shared";
	private static final String PROP_SERVICE = "io.shotgun.service";
	private static final String PROP_DNS = "io.shotgun.dns";
	private Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();
	private Duration[] publishRetryIn;
	private OneMQClient mqClient;
	private boolean manualAck;
	private boolean shared;
	private String service;
	private String dns;

	@Inject
	public ShotgunEventQueueProvider(Configuration config) {
		logger.debug("Shotgun Event Queue Provider init");

		manualAck = Boolean.parseBoolean(config.getProperty(PROP_MANUAL_ACK, "false"));
		shared = Boolean.parseBoolean(config.getProperty(PROP_SHARED, "false"));

		service = config.getProperty(PROP_SERVICE, null);
		if (StringUtils.isEmpty(service)) {
			throw new RuntimeException("No " + PROP_SERVICE + " property configured");
		}

		dns = config.getProperty(PROP_DNS, null);
		if (StringUtils.isEmpty(dns)) {
			throw new RuntimeException("No " + PROP_DNS + " property configured");
		}

		String[] arr = config.getProperty("io.shotgun.publishRetryIn", ",").split(",");
		publishRetryIn = new Duration[arr.length];
		for (int i = 0; i < arr.length; i++) {
			publishRetryIn[i] = Duration.ofSeconds(Long.parseLong(arr[i]));
		}

		logger.debug("Shotgun Event Queue Provider settings are dns=" + dns + ", service=" + service
				+ ", publishRetryIn=" + ArrayUtils.toString(publishRetryIn));

		if (shared) {
			try {
				mqClient = new OneMQ();
				mqClient.connect(dns, null, null);
			} catch (Exception ex) {
				logger.error("OneMQ client connect failed {}", ex.getMessage(), ex);
			}
			ScheduledExecutorService execs = Executors.newScheduledThreadPool(1);
			execs.scheduleAtFixedRate(this::monitor, 0, 500, TimeUnit.MILLISECONDS);
		}

		EventQueues.registerProvider(QueueType.shotgun, this);
		logger.debug("Shotgun Event Queue Provider initialized...");
	}

	@Override
	public ObservableQueue getQueue(String queueURI) {
		if (shared) {
			return queues.computeIfAbsent(queueURI, q -> new SharedShotgunQueue(mqClient, service, queueURI, publishRetryIn, manualAck));
		} else {
			return queues.computeIfAbsent(queueURI, q -> new ShotgunQueue(dns, service, queueURI, publishRetryIn, manualAck));
		}
	}

	@Override
	public void remove(String queueURI) {
		ObservableQueue queue = queues.get(queueURI);
		if (queue != null) {
			queue.close();
			queues.remove(queueURI);
			logger.debug("Remove initiated for queueURI = " + queueURI);
		}
	}

	private void monitor() {
		if (mqClient.isConnected()) {
			return;
		}
		try {
			mqClient.close();
			mqClient.connect(dns, null, null);
		} catch (Exception ex) {
			logger.error("OneMQ client connect failed {}", ex.getMessage(), ex);
		}
	}
}

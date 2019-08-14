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
package com.netflix.conductor.core.events;

import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Viren
 * Static holders for internal event queues
 */
public class EventQueues {

	private static Logger logger = LoggerFactory.getLogger(EventQueues.class);

	private static ParametersUtils pu = new ParametersUtils();

	public enum QueueType {
		sqs, conductor, nats, nats_stream, shotgun
	}

	private static Map<QueueType, EventQueueProvider> providers = new HashMap<>();

	private EventQueues() {

	}

	public static void registerProvider(QueueType type, EventQueueProvider provider) {
		providers.put(type, provider);
	}

	public static List<String> providers() {
		return providers.values().stream().map(p -> p.getClass().getName()).collect(Collectors.toList());
	}

	public static ObservableQueue getQueue(String eventt, boolean throwException) {
		String event = pu.replace(eventt).toString();
		String typeVal = event.substring(0, event.indexOf(':'));
		String queueURI = event.substring(event.indexOf(':') + 1);
		QueueType type = QueueType.valueOf(typeVal);
		EventQueueProvider provider = providers.get(type);
		if (provider != null) {
			try {
				return provider.getQueue(queueURI);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				if (throwException) {
					throw e;
				}
			}
		}

		return null;
	}

	public static ObservableQueue getQueue(String eventt, boolean throwException,
										   boolean manualAck, int prefetchSize) {
		String event = pu.replace(eventt).toString();
		String typeVal = event.substring(0, event.indexOf(':'));
		String queueURI = event.substring(event.indexOf(':') + 1);
		QueueType type = QueueType.valueOf(typeVal);
		EventQueueProvider provider = providers.get(type);
		if (provider != null) {
			try {
				return provider.getQueue(queueURI, manualAck, prefetchSize);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				if (throwException) {
					throw e;
				}
			}
		}

		return null;
	}

	public static void remove(String eventt) {
		String event = pu.replace(eventt).toString();
		String typeVal = event.substring(0, event.indexOf(':'));
		String queueURI = event.substring(event.indexOf(':') + 1);
		QueueType type = QueueType.valueOf(typeVal);
		EventQueueProvider provider = providers.get(type);
		if (provider != null) {
			provider.remove(queueURI);
		}
	}
}

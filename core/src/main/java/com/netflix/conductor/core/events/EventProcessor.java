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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventExecution.Status;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author Viren
 * Event Processor is used to dispatch actions based on the incoming events to execution queue.
 */
@Singleton
public class EventProcessor {
	private static String EVENT_BUS_VAR = "${event_bus}";
	private static Logger logger = LoggerFactory.getLogger(EventProcessor.class);

	private MetadataService ms;

	private ExecutionService es;

	private ActionProcessor ap;

	private Map<String, ObservableQueue> queuesMap = new ConcurrentHashMap<>();

	private ExecutorService executors;

	private ObjectMapper om;

	private String eventBus;

	@Inject
	public EventProcessor(ExecutionService es, MetadataService ms, ActionProcessor ap, Configuration config, ObjectMapper om) {
		this.es = es;
		this.ms = ms;
		this.ap = ap;
		this.om = om;
		this.eventBus = config.getProperty("event_bus", null);

		int executorThreadCount = config.getIntProperty("workflow.event.processor.thread.count", 2);

		// default 60 for backward compatibility
		int initialDelay = config.getIntProperty("workflow.event.processor.initial.delay", 60);
		int refreshPeriod = config.getIntProperty("workflow.event.processor.refresh.seconds", 60);
		if (executorThreadCount > 0) {
			this.executors = Executors.newFixedThreadPool(executorThreadCount);
			refresh();
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::refresh, initialDelay, refreshPeriod, TimeUnit.SECONDS);
		} else {
			logger.warn("Event processing is DISABLED.  executorThreadCount set to {}", executorThreadCount);
		}
	}

	/**
	 *
	 * @return Returns a map of queues which are active.  Key is event name and value is queue URI
	 */
	public Map<String, String> getQueues() {
		Map<String, String> queues = new HashMap<>();
		queuesMap.entrySet().stream().forEach(q -> queues.put(q.getKey(), q.getValue().getName()));
		return queues;
	}

	public Map<String, Map<String, Long>> getQueueSizes() {
		Map<String, Map<String, Long>> queues = new HashMap<>();
		queuesMap.entrySet().stream().forEach(q -> {
			Map<String, Long> size = new HashMap<>();
			size.put(q.getValue().getName(), q.getValue().size());
			queues.put(q.getKey(), size);
		});
		return queues;
	}

	public void refresh() {
		Set<String> events = ms.getEventHandlers().stream().filter(EventHandler::isActive).map(EventHandler::getEvent).map(this::handleEventBus).collect(Collectors.toSet());

		List<ObservableQueue> created = new LinkedList<>();
		events.forEach(event -> queuesMap.computeIfAbsent(event, s -> {
			ObservableQueue q = EventQueues.getQueue(event, false);
			created.add(q);
			return q;
		}));
		if (!created.isEmpty()) {
			created.stream().filter(Objects::nonNull).forEach(this::listen);
		}

		// Find events which present in queuesMap but does not exist in the db (disabled/removed)
		List<String> removed = new LinkedList<>();
		queuesMap.keySet().stream().filter(event -> !events.contains(event)).forEach(removed::add);

		// Close found entries
		removed.forEach(event -> {
			queuesMap.remove(event);
			EventQueues.remove(event);
		});
	}

	private String handleEventBus(String event) {
		if (StringUtils.isEmpty(eventBus)) {
			return event;
		}
		return event.replace(EVENT_BUS_VAR, eventBus);
	}

	private void listen(ObservableQueue queue) {
		queue.observe().subscribe((Message msg) -> handle(queue, msg));
	}

	private void handle(ObservableQueue queue, Message msg) {

		NDC.push("event-"+msg.getId());
		try {

			List<Future<Void>> futures = new LinkedList<>();

			String payload = msg.getPayload();
			Object payloadObj = null;
			if (payload != null) {
				try {
					payloadObj = om.readValue(payload, Object.class);
				} catch (Exception e) {
					payloadObj = payload;
				}
			}

			es.addMessage(queue.getName(), msg);

			// Find event handlers with direct event bus name as the prefix based on queue type
			String event = queue.getType() + ":" + queue.getName();
			List<EventHandler> handlers = ms.getEventHandlersForEvent(event, true);

			// Find additional event handler which starts with ${event_bus} ...
			if (StringUtils.isNotEmpty(eventBus)) {
				event = EVENT_BUS_VAR + ":" + queue.getName();
				handlers.addAll(ms.getEventHandlersForEvent(event, true));
			}

			for (EventHandler handler : handlers) {

				String condition = handler.getCondition();
				logger.debug("condition: {}", condition);
				if (!StringUtils.isEmpty(condition)) {
					Boolean success = ScriptEvaluator.evalBool(condition, payloadObj);
					if (!success) {
						logger.warn("handler {} condition {} did not match payload {}", handler.getName(), condition, payloadObj);
						EventExecution ee = new EventExecution(msg.getId() + "_0", msg.getId());
						ee.setCreated(System.currentTimeMillis());
						ee.setEvent(handler.getEvent());
						ee.setName(handler.getName());
						ee.setStatus(Status.SKIPPED);
						ee.getOutput().put("msg", payload);
						ee.getOutput().put("condition", condition);
						es.addEventExecution(ee);
						continue;
					}
				}

				int i = 0;
				List<Action> actions = handler.getActions();
				for (Action action : actions) {
					String id = msg.getId() + "_" + i++;
					if (StringUtils.isNotEmpty(action.getCondition())) {
						Boolean success = ScriptEvaluator.evalBool(action.getCondition(), payloadObj);
						if (!success) {
							logger.debug("{} condition {} did not match payload {}", action, condition, payloadObj);
							EventExecution ee = new EventExecution(id, msg.getId());
							ee.setCreated(System.currentTimeMillis());
							ee.setEvent(handler.getEvent());
							ee.setName(handler.getName());
							ee.setAction(action.getAction());
							ee.setStatus(Status.SKIPPED);
							ee.getOutput().put("msg", payload);
							ee.getOutput().put("condition", condition);
							es.addEventExecution(ee);
							continue;
						}
					}

					EventExecution ee = new EventExecution(id, msg.getId());
					ee.setCreated(System.currentTimeMillis());
					ee.setEvent(handler.getEvent());
					ee.setName(handler.getName());
					ee.setAction(action.getAction());
					ee.setStatus(Status.IN_PROGRESS);
					if (es.addEventExecution(ee)) {
						Future<Void> future = execute(ee, action, payload);
						futures.add(future);
					} else {
						logger.warn("Duplicate delivery/execution? {}", id);
					}
				}
			}

			for (Future<Void> future : futures) {
				try {
					future.get();
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}

			queue.ack(Arrays.asList(msg));

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			NDC.remove();
			Monitors.recordEventQueueMessagesProcessed(queue.getType(), queue.getName(), 1);
		}
	}

	private Future<Void> execute(EventExecution ee, Action action, String payload) {
		return executors.submit(() -> {
			NDC.push("event-"+ee.getMessageId());
			try {

				logger.debug("Executing {} with payload {}", action, payload);
				Map<String, Object> output = ap.execute(action, payload, ee.getEvent(), ee.getMessageId());
				if (output != null) {
					ee.getOutput().putAll(output);
				}
				ee.setStatus(Status.COMPLETED);
				es.updateEventExecution(ee);

				return null;
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				ee.setStatus(Status.FAILED);
				ee.getOutput().put("exception", e.getMessage());
				es.updateEventExecution(ee);

				return null;
			} finally {
				NDC.remove();
			}
		});
	}
}

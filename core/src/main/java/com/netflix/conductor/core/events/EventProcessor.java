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
import org.apache.commons.lang.BooleanUtils;
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

			List<Future<Boolean>> futures = new LinkedList<>();

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

			String subject = queue.getURI();
			if (queue.getURI().contains(":")) {
				subject = queue.getURI().substring(0, queue.getURI().indexOf(':'));
			}

			boolean retryMode = false;
			int tagsMatchCounter = 0;
			int tagsNotMatchCounter = 0;
			for (EventHandler handler : handlers) {

				String condition = handler.getCondition();
				if (StringUtils.isNotEmpty(condition)) {
					Boolean success = ScriptEvaluator.evalBool(condition, payloadObj);
					if (!success) {
						logger.warn("Handler did not match payload. Handler={}, condition={}, payload={}", handler.getName(), condition, payloadObj);
						EventExecution ee = new EventExecution(msg.getId() + "_0", msg.getId());
						ee.setCreated(System.currentTimeMillis());
						ee.setReceived(msg.getReceived());
						ee.setEvent(handler.getEvent());
						ee.setName(handler.getName());
						ee.setStatus(Status.SKIPPED);
						ee.getOutput().put("msg", payload);
						ee.getOutput().put("condition", condition);
						ee.setSubject(subject);
						es.addEventExecution(ee);
						continue;
					}
				}

				if (StringUtils.isNotEmpty(handler.getTags())) {
					retryMode = true;
					List<Object> candidates = ScriptEvaluator.evalJqAsList(handler.getTags(), payloadObj);
					Set<String> tags = candidates.stream().filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toSet());
					logger.debug("Evaluated tags={}", tags);

					boolean anyRunning = es.anyRunningWorkflowsByTags(tags);
					if (!anyRunning) {
						logger.debug("Handler did not find running workflows with tags. Handler={}, tags={}", handler.getName(), tags);
						EventExecution ee = new EventExecution(msg.getId() + "_0", msg.getId());
						ee.setCreated(System.currentTimeMillis());
						ee.setReceived(msg.getReceived());
						ee.setEvent(handler.getEvent());
						ee.setName(handler.getName());
						ee.setStatus(Status.SKIPPED);
						ee.getOutput().put("msg", payload);
						ee.getOutput().put("tags", tags);
						ee.setSubject(subject);
						es.addEventExecution(ee);
						tagsNotMatchCounter++;
						continue;
					} else  {
						tagsMatchCounter++;
					}
				}

				int i = 0;
				List<Action> actions = handler.getActions();
				for (Action action : actions) {
					String id = msg.getId() + "_" + i++;
					if (StringUtils.isNotEmpty(action.getCondition())) {
						Boolean success = ScriptEvaluator.evalBool(action.getCondition(), payloadObj);
						if (!success) {
							logger.debug("Action did not match payload. Handler={}, action={}, payload={}", handler.getName(), action, payloadObj);
							EventExecution ee = new EventExecution(id, msg.getId());
							ee.setCreated(System.currentTimeMillis());
							ee.setReceived(msg.getReceived());
							ee.setEvent(handler.getEvent());
							ee.setName(handler.getName());
							ee.setAction(action.getAction());
							ee.setStatus(Status.SKIPPED);
							ee.getOutput().put("msg", payload);
							ee.getOutput().put("condition", action.getCondition());
							ee.setSubject(subject);
							es.addEventExecution(ee);
							continue;
						}
					}

					EventExecution ee = new EventExecution(id, msg.getId());
					ee.setCreated(System.currentTimeMillis());
					ee.setReceived(msg.getReceived());
					ee.setEvent(handler.getEvent());
					ee.setName(handler.getName());
					ee.setAction(action.getAction());
					ee.setStatus(Status.IN_PROGRESS);
					ee.setSubject(subject);
					if (es.addEventExecution(ee)) {
						Future<Boolean> future = execute(ee, action, payload);
						futures.add(future);
					} else {
						logger.warn("Duplicate delivery/execution? {}", id);
					}
				}
			}

			// if no tags for all handlers - ack message
			if (tagsNotMatchCounter > 0 && tagsMatchCounter == 0) {
				logger.debug("No running workflows for the tags. Ack for " + msg.getReceipt());
				queue.ack(Collections.singletonList(msg));
				return;
			}

			boolean anySuccess = false;
			for (Future<Boolean> future : futures) {
				try {
					if (BooleanUtils.isTrue(future.get())) {
						anySuccess = true;
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}

			// Ack for legacy mode
			if (!retryMode) {
				logger.debug("Ack for messageId=" + msg.getReceipt());
				queue.ack(Collections.singletonList(msg));
			} else {
				// Any action succeeded
				if (anySuccess) {
					logger.debug("Processed. Ack for messageId=" + msg.getReceipt());
					queue.ack(Collections.singletonList(msg));
				} else {
					logger.debug("Redelivery needed. Unack for messageId=" + msg.getReceipt());
					queue.unack(Collections.singletonList(msg));
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			queue.unack(Collections.singletonList(msg));
		} finally {
			NDC.remove();
			Monitors.recordEventQueueMessagesProcessed(queue.getType(), queue.getName(), 1);
		}
	}

	private Future<Boolean> execute(EventExecution ee, Action action, String payload) {
		return executors.submit(() -> {
			boolean success = false;
			NDC.push("event-"+ee.getMessageId());
			try {
				logger.debug("Starting handler=" + ee.getName() + ", action=" + action + ", payload=" + payload);
				Map<String, Object> output = ap.execute(action, payload, ee.getEvent(), ee.getMessageId());
				if (output != null) {
					ee.getOutput().putAll(output);
					success = BooleanUtils.isTrue((Boolean) output.get("conductor.event.success"));
				}
				ee.setStatus(Status.COMPLETED);
				ee.setProcessed(System.currentTimeMillis());
				es.updateEventExecution(ee);

				long execTime = System.currentTimeMillis() - ee.getCreated();
				logger.debug("Executed handler=" + ee.getName() + ", action=" + action +
					", payload=" + payload + ", success=" + success + ", execTime=" + execTime);
				return success;
			} catch (Exception e) {
				logger.debug("Execute failed handler=" + ee.getName() + ", action=" + action +
					", payload=" + payload + ", reason=" + e.getMessage(), e);
				ee.setStatus(Status.FAILED);
				ee.getOutput().put("exception", e.getMessage());
				ee.setProcessed(System.currentTimeMillis());
				es.updateEventExecution(ee);

				return success;
			} finally {
				NDC.remove();
			}
		});
	}
}

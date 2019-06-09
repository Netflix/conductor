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
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.*;

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
			msg.setAccepted(System.currentTimeMillis());

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
			if (isNotEmpty(eventBus)) {
				event = EVENT_BUS_VAR + ":" + queue.getName();
				handlers.addAll(ms.getEventHandlersForEvent(event, true));
			}

			String subject = queue.getURI();
			if (queue.getURI().contains(":")) {
				subject = queue.getURI().substring(0, queue.getURI().indexOf(':'));
			}

			// The retry flag is true if ANY of handlers requires it and tags JQ expression defined
			boolean retryEnabled = handlers.stream().anyMatch(h -> h.isRetryEnabled() && StringUtils.isNotEmpty(h.getTags()));
			int tagsMatchCounter = 0;
			int tagsNotMatchCounter = 0;
			Set<String> tags = null;
			for (EventHandler handler : handlers) {
				// Check handler's condition
				String condition = handler.getCondition();
				String conditionClass = handler.getConditionClass();
				if (isNotEmpty(condition) || isNotEmpty(conditionClass)) {
					boolean success = evalCondition(condition, conditionClass, payloadObj);
					if (!success) {
						logger.debug("Handler did not match payload. Handler={}, condition={}", handler.getName(), condition);
						EventExecution ee = new EventExecution(msg.getId() + "_0", msg.getId());
						ee.setAccepted(msg.getAccepted());
						ee.setCreated(System.currentTimeMillis());
						ee.setReceived(msg.getReceived());
						ee.setEvent(handler.getEvent());
						ee.setName(handler.getName());
						ee.setStatus(Status.SKIPPED);
						ee.getOutput().put("msg", payload);
						ee.setSubject(subject);
						es.addEventExecution(ee);
						continue;
					}
				}

				// Evaluate tags and check associated workflows (if needed)
				if (isNotEmpty(handler.getTags())) {
					List<Object> candidates = ScriptEvaluator.evalJqAsList(handler.getTags(), payloadObj);
					tags = candidates.stream().filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toSet());
					logger.debug("Evaluated tags={}", tags);

					// Check running workflows only when retry enabled
					// because outcome of that check used by retry decider ...
					// see below when ack or unack decided
					if (retryEnabled) {
						boolean anyRunning = es.anyRunningWorkflowsByTags(tags);
						if (!anyRunning) {
							logger.debug("Handler did not find running workflows with tags. Handler={}, tags={}", handler.getName(), tags);
							EventExecution ee = new EventExecution(msg.getId() + "_0", msg.getId());
							ee.setAccepted(msg.getAccepted());
							ee.setCreated(System.currentTimeMillis());
							ee.setReceived(msg.getReceived());
							ee.setEvent(handler.getEvent());
							ee.setName(handler.getName());
							ee.setStatus(Status.SKIPPED);
							ee.getOutput().put("msg", payload);
							ee.setSubject(subject);
							ee.setTags(tags);
							es.addEventExecution(ee);
							tagsNotMatchCounter++;
							continue;
						} else  {
							tagsMatchCounter++;
						}
					}
				}

				// Walk over the handler's actions
				int i = 0;
				List<Action> actions = handler.getActions();
				for (Action action : actions) {
					String id = msg.getId() + "_" + i++;

					condition = action.getCondition();
					conditionClass = action.getConditionClass();
					if (isNotEmpty(condition) || isNotEmpty(conditionClass)) {
						boolean success = evalCondition(condition, conditionClass, payloadObj);
						if (!success) {
							logger.debug("Action did not match payload. Handler={}, action={}", handler.getName(), action);
							EventExecution ee = new EventExecution(id, msg.getId());
							ee.setAccepted(msg.getAccepted());
							ee.setCreated(System.currentTimeMillis());
							ee.setReceived(msg.getReceived());
							ee.setEvent(handler.getEvent());
							ee.setName(handler.getName());
							ee.setAction(action.getAction());
							ee.setStatus(Status.SKIPPED);
							ee.getOutput().put("msg", payload);
							ee.setSubject(subject);
							ee.setTags(tags);
							es.addEventExecution(ee);
							continue;
						}
					}

					EventExecution ee = new EventExecution(id, msg.getId());
					ee.setAccepted(msg.getAccepted());
					ee.setCreated(System.currentTimeMillis());
					ee.setReceived(msg.getReceived());
					ee.setEvent(handler.getEvent());
					ee.setName(handler.getName());
					ee.setAction(action.getAction());
					ee.setStatus(Status.IN_PROGRESS);
					ee.setSubject(subject);
					ee.setTags(tags);
					if (es.addEventExecution(ee)) {
						Future<Boolean> future = execute(ee, action, payload);
						futures.add(future);
					} else {
						logger.warn("Duplicate delivery/execution? {}", id);
					}
				}
			}

			// if no tags for all handlers - ack message
			if (retryEnabled && tagsNotMatchCounter > 0 && tagsMatchCounter == 0) {
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
			if (!retryEnabled) {
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

	private boolean evalCondition(String condition, String conditionClass, Object payload) throws Exception {
		if (isNotEmpty(condition)) {
			return ScriptEvaluator.evalBool(condition, payload);
		} else if (isNotEmpty(conditionClass)) {
			Class clazz = Class.forName(conditionClass);
			JavaEventCondition javaEventCondition = (JavaEventCondition) clazz.newInstance();
			return javaEventCondition.evalBool(payload);
		}
		return true;
	}

	private Future<Boolean> execute(EventExecution ee, Action action, String payload) {
		return executors.submit(() -> {
			boolean success = false;
			NDC.push("event-"+ee.getMessageId());
			try {
				logger.debug("Starting handler=" + ee.getName() + ", action=" + action);
				ee.setStarted(System.currentTimeMillis());
				Map<String, Object> output = ap.execute(action, payload, ee);
				if (output != null) {
					ee.getOutput().putAll(output);
					success = BooleanUtils.isTrue((Boolean) output.get("conductor.event.success"));
				}
				ee.setStatus(Status.COMPLETED);
				ee.setProcessed(System.currentTimeMillis());
				es.updateEventExecution(ee);

				// Wait for accepting by event processor
				long waitTime = ee.getAccepted() - ee.getReceived();

				// Preparation time. Between accepted and actually submitting for execution
				long prepTime = ee.getCreated() - ee.getAccepted();

				// Action execution time
				long execTime = ee.getProcessed() - ee.getStarted();

				// Overall time for the message
				long overallTime = System.currentTimeMillis() - ee.getReceived();
				logger.debug("Executed handler=" + ee.getName() + ", action=" + action + ", success=" + success +
					", waitTime=" + waitTime + ", prepTime=" + prepTime +
					", execTime=" + execTime + ", overallTime=" + overallTime);
				return success;
			} catch (Exception e) {
				logger.debug("Execute failed handler=" + ee.getName() + ", action=" + action + ", reason=" + e.getMessage(), e);
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

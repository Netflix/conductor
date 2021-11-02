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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventExecution.Status;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import com.netflix.conductor.service.MetricService;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * @author Viren
 * Event Processor is used to dispatch actions based on the incoming events to execution queue.
 */
@Singleton
public class EventProcessor {
	private static Logger logger = LoggerFactory.getLogger(EventProcessor.class);
	private Map<String, Pair<ObservableQueue, ThreadPoolExecutor>> queuesMap = new ConcurrentHashMap<>();
	private ParametersUtils pu = new ParametersUtils();
	private volatile List<EventHandler> activeHandlers;
	private ScheduledExecutorService refreshPool;
	private MetadataService ms;
	private ExecutionService es;
	private ActionProcessor ap;
	private ObjectMapper om;

	@Inject
	public EventProcessor(ExecutionService es, MetadataService ms, ActionProcessor ap, Configuration config, ObjectMapper om) {
		this.es = es;
		this.ms = ms;
		this.ap = ap;
		this.om = om;

		boolean disabled = Boolean.parseBoolean(config.getProperty("workflow.event.processor.disabled", "false"));
		if (!disabled) {
			refresh();

			int initialDelay = config.getIntProperty("workflow.event.processor.initial.delay", 60);
			int refreshPeriod = config.getIntProperty("workflow.event.processor.refresh.seconds", 60);
			refreshPool = Executors.newScheduledThreadPool(1);
			refreshPool.scheduleWithFixedDelay(this::refresh, initialDelay, refreshPeriod, TimeUnit.SECONDS);
		} else {
			logger.debug("Event processing is DISABLED");
		}
	}

	/**
	 *
	 * @return Returns a map of queues which are active.  Key is event name and value is queue URI
	 */
	public Map<String, String> getQueues() {
		Map<String, String> queues = new HashMap<>();
		queuesMap.entrySet().stream().forEach(q -> queues.put(q.getKey(), q.getValue().getLeft().getName()));
		return queues;
	}

	public Map<String, Map<String, Long>> getQueueSizes() {
		Map<String, Map<String, Long>> queues = new HashMap<>();
		queuesMap.entrySet().stream().forEach(q -> {
			Map<String, Long> size = new HashMap<>();
			size.put(q.getValue().getLeft().getName(), q.getValue().getLeft().size());
			queues.put(q.getKey(), size);
		});
		return queues;
	}

	public synchronized void refresh() {
		try {
			activeHandlers = ms.getEventHandlers().stream().filter(EventHandler::isActive)
				.peek(handler -> {
					String replaced = (String) pu.replace(handler.getEvent());
					handler.setEvent(replaced);
				}).collect(Collectors.toList());

			List<ObservableQueue> created = new LinkedList<>();
			activeHandlers.parallelStream().forEach(handler -> queuesMap.computeIfAbsent(handler.getEvent(), s -> {
				//validate handler/action conditions
				validateHandlerConditions(handler);

				ObservableQueue queue = EventQueues.getQueue(handler.getEvent(), false,
						handler.isRetryEnabled(), handler.getPrefetchSize(), this::handle);

				if (queue == null) {
					return null;
				}
				created.add(queue);

				logger.debug("Creating " + handler.getThreadCount() + " executors for " + handler.getName());
				ThreadPoolExecutor executor = new ThreadPoolExecutor(handler.getThreadCount(), handler.getThreadCount(),
					0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

				return Pair.of(queue, executor);
			}));

			// Init subscription (if not yet done)
			if (!created.isEmpty()) {
				created.stream().filter(Objects::nonNull).forEach(ObservableQueue::observe);
			}

			// Find events which present in queuesMap but does not exist in the db (disabled/removed)
			List<String> removed = new LinkedList<>();
			queuesMap.keySet().forEach(event -> {
				boolean noneMatch = activeHandlers.stream().noneMatch(handler -> handler.getEvent().equalsIgnoreCase(event));
				if (noneMatch) {
					removed.add(event);
				}
			});

			// Close required  and remove from the mapping
			removed.forEach(event -> queuesMap.computeIfPresent(event, (s, entry) -> {
				closeQueue(event);
				closeExecutor(event, entry.getRight());
				return null;
			}));

			// Any prefetchSize or threadCount changed
			// - close subscription to keep messages at provider
			// - close/open executor
			// - open subscription
			List<ObservableQueue> changed = new LinkedList<>();
			activeHandlers.parallelStream().forEach(handler -> queuesMap.computeIfPresent(handler.getEvent(), (s, entry) -> {
				if (handler.getThreadCount() != entry.getRight().getCorePoolSize() ||
					handler.getPrefetchSize() != entry.getLeft().getPrefetchSize()) {
					logger.debug("Re-creating queue/executor for " + handler.getName());

					closeQueue(handler.getEvent());
					closeExecutor(handler.getEvent(), entry.getRight());

					logger.debug("Creating " + handler.getThreadCount() + " executors for " + handler.getName());
					ThreadPoolExecutor executor = new ThreadPoolExecutor(handler.getThreadCount(), handler.getThreadCount(),
						0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

					// Init the queue back
					ObservableQueue queue = EventQueues.getQueue(handler.getEvent(), false,
						handler.isRetryEnabled(), handler.getPrefetchSize(), this::handle);
					changed.add(queue);

					return Pair.of(queue, executor);
				}
				return entry;
			}));

			// Init subscription
			if (!changed.isEmpty()) {
				changed.stream().filter(Objects::nonNull).forEach(ObservableQueue::observe);
			}

		} catch (Exception ex) {
			logger.debug("refresh failed " + ex.getMessage(), ex);
		}
	}

	public void shutdown() {
		try {
			if (refreshPool != null) {
				logger.info("Closing refresh pool");
				refreshPool.shutdown();
				refreshPool.awaitTermination(5, TimeUnit.SECONDS);
			}
		} catch (Exception e) {
			logger.debug("Closing refresh pool failed " + e.getMessage(), e);
		}
		try {
			if (!queuesMap.isEmpty()) {
				logger.info("Closing queues & executors");
				queuesMap.entrySet().parallelStream().forEach(entry -> {
					closeQueue(entry.getKey());
					closeExecutor(entry.getKey(), entry.getValue().getRight());
				});
			}
		} catch (Exception e) {
			logger.debug("Closing queues & executors failed " + e.getMessage(), e);
		}
	}

	private void closeQueue(String event) {
		logger.debug("Closing queue " + event);
		try {
			EventQueues.remove(event);
		} catch (Exception ex) {
			logger.debug("Queue closed failed for " + event + " " + ex.getMessage(), ex);
		}
	}

	private void closeExecutor(String event, ThreadPoolExecutor executor) {
		logger.debug("Closing executor " + event);
		try {
			executor.shutdownNow();
			executor.awaitTermination(1, TimeUnit.SECONDS);
		} catch (Exception ex) {
			logger.debug("Executor close failed for " + event + " " + ex.getMessage(), ex);
		}
	}

	private void handle(ObservableQueue queue, Message msg) {
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

			// Find event handlers by the event name considering variables in the handler's event
			String event = queue.getType() + ":" + queue.getURI();
			List<EventHandler> handlers = activeHandlers.stream()
				.filter(handler -> handler.getEvent().equalsIgnoreCase(event))
				.collect(Collectors.toList());

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
						MetricService.getInstance()
							.eventExecutionSkipped(handler.getName(), queue.getSubject());
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
							MetricService.getInstance()
								.eventTagsMiss(handler.getName(), queue.getSubject());
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
						} else {
							MetricService.getInstance()
								.eventTagsHit(handler.getName(), queue.getSubject());
							tagsMatchCounter++;
						}
					}
				}

				// Walk over the handler's actions
				int i = 0;
				List<Action> actions = handler.getActions();
				for (Action action : actions) {
					String actionName = action.getAction().name() + "_" + i;
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
							MetricService.getInstance()
								.eventActionSkipped(handler.getName(), queue.getSubject(), actionName);
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
					ExecutorService executor = queuesMap.get(event).getRight();
					Future<Boolean> future = execute(executor, ee, action, payload);
					futures.add(future);
				}
			}

			// if no tags for all handlers - ack message
			if (retryEnabled && tagsNotMatchCounter > 0 && tagsMatchCounter == 0) {
				logger.debug("No running workflows for the tags. Ack for " + msg.getReceipt());
				//logger.info("ShotgunMsg:ep:  ACKing Message. No wf " + msg.getReceipt());
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
					//logger.info("ShotgunMsg:ep: Process Message. Failed " + msg.getReceipt(), e);
					logger.error(e.getMessage(), e);
				}
			}

			// Ack for legacy mode or when no actions submitted (e.g. handler/actions did not match payload)
			if (!retryEnabled || futures.isEmpty()) {
				logger.debug("Ack for messageId=" + msg.getReceipt());
				//logger.info("ShotgunMsg:ep: ACKing Message.. No match " + msg.getReceipt());
				queue.ack(Collections.singletonList(msg));
			} else {
				// Any action succeeded
				if (anySuccess) {
					logger.debug("Processed. Ack for messageId=" + msg.getReceipt());
					//logger.info("ShotgunMsg:ep: ACKing Message. Processed " + msg.getReceipt());
					queue.ack(Collections.singletonList(msg));
				} else {
					MetricService.getInstance()
						.eventRedeliveryRequested(queue.getName(), queue.getSubject());
					logger.debug("Redelivery needed. Unack for messageId=" + msg.getReceipt());
					//logger.info("ShotgunMsg:ep: UNACK Message. Need redelivery " + msg.getReceipt());
					queue.unack(Collections.singletonList(msg));
				}
			}
		} catch (Exception e) {
			MetricService.getInstance()
				.eventExecutionFailed(queue.getName(), queue.getSubject());
			logger.error(e.getMessage() + " occurred for " + msg.getPayload(), e);
			//logger.info("ShotgunMsg:ep: UNACK Message.. Exception " + msg.getReceipt(), e);
			queue.unack(Collections.singletonList(msg));
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

	private Future<Boolean> execute(ExecutorService executor, EventExecution ee, Action action, String payload) {
		return executor.submit(() -> {
			boolean success = false;
			NDC.push("event-" + ee.getMessageId());
			try {
				logger.debug("Starting handler=" + ee.getName() + ", action=" + action);
				if (!es.addEventExecution(ee)) {
					logger.debug("Duplicate delivery/execution? {}", ee.getId());
				}
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
					", execTime=" + execTime + ", overallTime=" + overallTime +
					", output=" + output);

				MetricService.getInstance()
					.eventActionExecuted(ee.getName(), ee.getSubject(), ee.getAction().name(), execTime);
				return success;
			} catch (Exception e) {
				logger.debug("Execute failed handler=" + ee.getName() + ", action=" + action + ", reason=" + e.getMessage(), e);
				ee.setStatus(Status.FAILED);
				ee.getOutput().put("exception", e.getMessage());
				ee.setProcessed(System.currentTimeMillis());
				es.updateEventExecution(ee);
				MetricService.getInstance()
					.eventActionFailed(ee.getName(), ee.getSubject(), ee.getAction().name());
				return success;
			} finally {
				NDC.remove();
			}
		});
	}

	public void validateHandlerConditions(EventHandler handler) {
		String condition = handler.getCondition();
		String conditionClass = handler.getConditionClass();
		ObjectNode payloadObj = om.createObjectNode();
		if (isNotEmpty(condition) || isNotEmpty(conditionClass)) {
			try {
				evalCondition(condition, conditionClass, payloadObj);
			} catch (Exception ex) {
				logger.error(handler.getName() + " event handler condition validation failed " + ex.getMessage(), ex);
			}
		}

		//Validate handler action conditions
		int i = 0;
		List<Action> actions = handler.getActions();
		for (Action action : actions) {
			String actionName = action.getAction().name() + "_" + i;
			String actionCondition = action.getCondition();
			String actionConditionClass = action.getConditionClass();
			ObjectNode actionPayloadObj = om.createObjectNode();
			if (isNotEmpty(actionCondition) || isNotEmpty(actionConditionClass)) {
				try {
					evalCondition(actionCondition, actionConditionClass, actionPayloadObj);
				} catch (Exception ex) {
					logger.error(handler.getName() + " event handler action " + actionName + " condition validation failed " + ex.getMessage(), ex);
				}
			}
		}
	}
}

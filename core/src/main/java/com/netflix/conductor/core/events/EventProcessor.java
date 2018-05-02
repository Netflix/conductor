/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.conductor.core.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventExecution.Status;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Viren
 * Event Processor is used to dispatch actions based on the incoming events to execution queue.
 */
@Singleton
public class EventProcessor {

	private static Logger logger = LoggerFactory.getLogger(EventProcessor.class);
	
	private MetadataService metadataService;
	
	private ExecutionService executionService;
	
	private ActionProcessor actionProcessor;
	
	private Map<String, ObservableQueue> queuesMap = new ConcurrentHashMap<>();
	
	private ExecutorService executorService;
	
	private ObjectMapper objectMapper;

	
	@Inject
	public EventProcessor(ExecutionService executionService, MetadataService metadataService,
						  ActionProcessor actionProcessor, Configuration config, ObjectMapper objectMapper) {
		this.executionService = executionService;
		this.metadataService = metadataService;
		this.actionProcessor = actionProcessor;
		this.objectMapper = objectMapper;

		int executorThreadCount = config.getIntProperty("workflow.event.processor.thread.count", 2);
		if(executorThreadCount > 0) {
			this.executorService = Executors.newFixedThreadPool(executorThreadCount);
			refresh();
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> refresh(), 60, 60, TimeUnit.SECONDS);
		} else {
			logger.warn("Event processing is DISABLED.  executorThreadCount set to {}", executorThreadCount);
		}
	}
	
	/**
	 * 
	 * @return Returns a map of queues which are active.
	 * Key is event name and value is queue URI
	 */
	public Map<String, String> getQueues() {
		Map<String, String> queues = new HashMap<>();
		queuesMap.forEach((key, value) -> queues.put(key, value.getName()));
		return queues;
	}

	/**
	 *
	 * @return Returns a map of queues with its sizes.
	 * Key is event name and value is a map of queue URI to queue size.
	 */
	public Map<String, Map<String, Long>> getQueueSizes() {
		Map<String, Map<String, Long>> queues = new HashMap<>();
		queuesMap.forEach((key, value) -> {
			Map<String, Long> size = new HashMap<>();
			size.put(value.getName(), value.size());
			queues.put(key, size);
		});
		return queues;
	}
	
	private void refresh() {
		Set<String> events = metadataService.getEventHandlers().stream().map(EventHandler::getEvent).collect(Collectors.toSet());
		List<ObservableQueue> created = new LinkedList<>();
		events.forEach(event -> queuesMap.computeIfAbsent(event, s -> {
			ObservableQueue q = EventQueues.getQueue(event, false);
			created.add(q);
			return q;
		}));
		if(!created.isEmpty()) {
			created.stream().filter(Objects::nonNull).forEach(this::listen);
		}
	}
	
	private void listen(ObservableQueue queue) {
		queue.observe().subscribe((Message msg) -> handle(queue, msg));
	}
	
	private void handle(ObservableQueue queue, Message msg) {
		try {
			
			List<Future<Void>> futures = new LinkedList<>();
			
			String payload = msg.getPayload();
			Object payloadObj = null;
			if(payload != null) {
				try {
					payloadObj = objectMapper.readValue(payload, Object.class);
				}catch(Exception e) {
					payloadObj = payload;
				}
			}
			
			executionService.addMessage(queue.getName(), msg);
			
			String event = queue.getType() + ":" + queue.getName();
			List<EventHandler> handlers = metadataService.getEventHandlersForEvent(event, true);
			
			for(EventHandler handler : handlers) {
				
				String condition = handler.getCondition();
				logger.debug("condition: {}", condition);
				if(!StringUtils.isEmpty(condition)) {
					Boolean success = ScriptEvaluator.evalBool(condition, payloadObj);
					if(!success) {
						logger.info("handler {} condition {} did not match payload {}", handler.getName(), condition, payloadObj);
						EventExecution ee = new EventExecution(msg.getId() + "_0", msg.getId());
						ee.setCreated(System.currentTimeMillis());
						ee.setEvent(handler.getEvent());
						ee.setName(handler.getName());
						ee.setStatus(Status.SKIPPED);
						ee.getOutput().put("msg", payload);
						ee.getOutput().put("condition", condition);
						executionService.addEventExecution(ee);
						continue;
					}
				}
				
				int i = 0;
				List<Action> actions = handler.getActions();
				for(Action action : actions) {
					String id = msg.getId() + "_" + i++;
					
					EventExecution ee = new EventExecution(id, msg.getId());
					ee.setCreated(System.currentTimeMillis());
					ee.setEvent(handler.getEvent());
					ee.setName(handler.getName());
					ee.setAction(action.getAction());
					ee.setStatus(Status.IN_PROGRESS);
					if (executionService.addEventExecution(ee)) {
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
		}
	}

	private Future<Void> execute(EventExecution ee, Action action, String payload) {
		return executorService.submit(()->{
			try {
				
				logger.debug("Executing {} with payload {}", action.getAction(), payload);
				Map<String, Object> output = actionProcessor.execute(action, payload, ee.getEvent(), ee.getMessageId());
				if(output != null) {
					ee.getOutput().putAll(output);
				}
				ee.setStatus(Status.COMPLETED);
				executionService.updateEventExecution(ee);
				
				return null;
			}catch(Exception e) {
				logger.error(e.getMessage(), e);
				ee.setStatus(Status.FAILED);
				ee.getOutput().put("exception", e.getMessage());
				return null;
			}
		});
	}
}

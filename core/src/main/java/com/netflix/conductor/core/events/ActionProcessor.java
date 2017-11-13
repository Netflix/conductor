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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.common.metadata.events.EventHandler.ProgressTask;
import com.netflix.conductor.common.metadata.events.EventHandler.StartWorkflow;
import com.netflix.conductor.common.metadata.events.EventHandler.TaskDetails;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.service.MetadataService;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Viren
 * Action Processor subscribes to the Event Actions queue and processes the actions (e.g. start workflow etc)
 * <p><font color=red>Warning</font> This is a work in progress and may be changed in future.  Not ready for production yet.
 */
@Singleton
public class ActionProcessor {

	private static Logger logger = LoggerFactory.getLogger(EventProcessor.class);

	private WorkflowExecutor executor;

	private MetadataService metadata;

	private static final ObjectMapper om = new ObjectMapper();

	private ParametersUtils pu = new ParametersUtils();

	private LoadingCache<String, JsonQuery> queryCache = createQueryCache();

	@Inject
	public ActionProcessor(WorkflowExecutor executor, MetadataService metadata) {
		this.executor = executor;
		this.metadata = metadata;
	}

	public Map<String, Object> execute(Action action, String payload, String event, String messageId) throws Exception {

		logger.debug("Executing {}", action.getAction());
		Object jsonObj = om.readValue(payload, Object.class);
		if (action.isExpandInlineJSON()) {
			jsonObj = expand(jsonObj);
		}

		switch (action.getAction()) {
			case start_workflow:
				Map<String, Object> op = startWorkflow(action, jsonObj, event, messageId);
				return op;
			case complete_task:
				op = completeTask(action, jsonObj, action.getComplete_task(), Status.COMPLETED, event, messageId);
				return op;
			case fail_task:
				op = completeTask(action, jsonObj, action.getFail_task(), Status.FAILED, event, messageId);
				return op;
			case progress_task:
				op = progressTask(jsonObj, action.getProgress_task());
				return op;
			default:
				break;
		}
		throw new UnsupportedOperationException("Action not supported " + action.getAction());

	}

	private Map<String, Object> completeTask(Action action, Object payload, TaskDetails taskDetails, Status status, String event, String messageId) {

		Map<String, Object> input = new HashMap<>();
		input.put("workflowId", taskDetails.getWorkflowId());
		input.put("taskRefName", taskDetails.getTaskRefName());
		input.putAll(taskDetails.getOutput());

		Map<String, Object> replaced = pu.replace(input, payload);
		String workflowId = "" + replaced.get("workflowId");
		String taskRefName = "" + replaced.get("taskRefName");
		Workflow found = executor.getWorkflow(workflowId, true);
		if (found == null) {
			replaced.put("error", "No workflow found with ID: " + workflowId);
			return replaced;
		}
		Task task = found.getTaskByRefName(taskRefName);
		if (task == null) {
			replaced.put("error", "No task found with reference name: " + taskRefName + ", workflowId: " + workflowId);
			return replaced;
		}

		task.setStatus(status);
		task.setOutputData(replaced);
		task.getOutputData().put("conductor.event.messageId", messageId);
		task.getOutputData().put("conductor.event.name", event);

		try {
			executor.updateTask(new TaskResult(task));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			replaced.put("error", e.getMessage());
		}
		return replaced;
	}

	private Map<String, Object> startWorkflow(Action action, Object payload, String event, String messageId) throws Exception {
		StartWorkflow params = action.getStart_workflow();
		Map<String, Object> op = new HashMap<>();
		try {

			WorkflowDef def = metadata.getWorkflowDef(params.getName(), params.getVersion());
			Map<String, Object> inputParams = params.getInput();
			Map<String, Object> workflowInput = pu.replace(inputParams, payload);
			workflowInput.put("conductor.event.messageId", messageId);
			workflowInput.put("conductor.event.name", event);

			String id = executor.startWorkflow(def.getName(), def.getVersion(), params.getCorrelationId(), workflowInput, event);
			op.put("workflowId", id);


		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			op.put("error", e.getMessage());
		}

		return op;
	}

	private Map<String, Object> progressTask(Object payload, ProgressTask progressTask) throws Exception {
		Map<String, Object> op = new HashMap<>();
		logger.info("progressTask: starting executing " + progressTask);
		try {
			String workflowId = evaluate(progressTask.getWorkflowId(), payload);
			if (StringUtils.isEmpty(workflowId))
				throw new RuntimeException("workflowId evaluating is empty");

			String taskId = evaluate(progressTask.getTaskId(), payload);
			if (StringUtils.isEmpty(taskId))
				throw new RuntimeException("taskId evaluating is empty");

			String status = evaluate(progressTask.getStatus(), payload);
			if (StringUtils.isEmpty(status))
				throw new RuntimeException("status evaluating is empty");

			String reasonForIncompletion = null;
			if (StringUtils.isNotEmpty(progressTask.getReasonForIncompletion())) {
				reasonForIncompletion = evaluate(progressTask.getReasonForIncompletion(), payload);
				if (StringUtils.isEmpty(reasonForIncompletion))
					throw new RuntimeException("reasonForIncompletion evaluating is empty");
			}

			Function<String, Status> getTaskStatus = s -> {
				try {
					return Status.valueOf(s);
				} catch (Exception ex) {
					logger.error("progressTask: getTaskStatus failed with " + ex.getMessage());
					return null;
				}
			};

			Status taskStatus;
			// If no mapping - then task status taken from status
			Map<String, String> mapping = progressTask.getMapping();
			if (mapping == null || mapping.isEmpty()) {
				taskStatus = getTaskStatus.apply(status.toUpperCase());
			} else {
				taskStatus = getTaskStatus.apply(mapping.get(status));
			}
			if (taskStatus == null)
				throw new RuntimeException("Unable to determine task status");

			Workflow workflow = executor.getWorkflow(workflowId, true);
			if (workflow == null)
				throw new RuntimeException("No workflow found with id " + workflowId);

			Task targetTask = workflow.getTasks().stream().filter(item -> taskId.equals(item.getTaskId()))
					.findFirst().orElse(null);
			if (targetTask == null)
				throw new RuntimeException("No task found with id " + taskId + " for workflow " + workflowId);

			targetTask.getOutputData().put("event", payload);
			targetTask.setStatus(taskStatus);

			// Set the reason if task failed. It should be provided in the event
			if (Task.Status.FAILED.equals(taskStatus)) {
				targetTask.setReasonForIncompletion(reasonForIncompletion);
			}

			// Create task update wrapper and reset timer if in-progress
			TaskResult taskResult = new TaskResult(targetTask);
			if (Task.Status.IN_PROGRESS.equals(taskStatus) && progressTask.getResetStartTime()) {
				taskResult.setResetStartTime(true);
			}

			// Let's update task
			executor.updateTask(taskResult);

			try {
				// Remove event handler only on terminal task status
				if (taskStatus.isTerminal()) {
					metadata.removeEventHandlerStatus("EVENT_WAIT." + workflowId + "." + taskId);
				}
			} catch (Exception ignore) {
			}
		} catch (Exception e) {
			logger.error("progressTask: failed with " + e.getMessage() + " for " + progressTask, e);
			op.put("error", e.getMessage());
		}

		return op;
	}

	private String evaluate(String expression, Object payload) throws Exception {
		JsonNode input = om.valueToTree(payload);
		JsonQuery query = queryCache.get(expression);
		List<JsonNode> result = query.apply(input);
		if (result == null) {
			return null;
		} else if (result.isEmpty()) {
			return null;
		} else {
			return result.get(0).asText();
		}
	}

	private Object getJson(String jsonAsString) {
		try {
			Object value = om.readValue(jsonAsString, Object.class);
			return value;
		} catch (Exception e) {
			return jsonAsString;
		}
	}

	@SuppressWarnings("unchecked")
	@VisibleForTesting
	Object expand(Object input) {
		if (input instanceof List) {
			expandList((List<Object>) input);
			return input;
		} else if (input instanceof Map) {
			expandMap((Map<String, Object>) input);
			return input;
		} else if (input instanceof String) {
			return getJson((String) input);
		} else {
			return input;
		}
	}

	@SuppressWarnings("unchecked")
	private void expandList(List<Object> input) {
		for (Object value : input) {
			if (value instanceof String) {
				value = getJson(value.toString());
			} else if (value instanceof Map) {
				expandMap((Map<String, Object>) value);
			} else if (value instanceof List) {
				expandList((List<Object>) value);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void expandMap(Map<String, Object> input) {
		for (Map.Entry<String, Object> e : input.entrySet()) {
			Object value = e.getValue();
			if (value instanceof String) {
				value = getJson(value.toString());
				e.setValue(value);
			} else if (value instanceof Map) {
				expandMap((Map<String, Object>) value);
			} else if (value instanceof List) {
				expandList((List<Object>) value);
			}
		}
	}

	private LoadingCache<String, JsonQuery> createQueryCache() {
		CacheLoader<String, JsonQuery> loader = new CacheLoader<String, JsonQuery>() {
			@Override
			public JsonQuery load(@Nonnull String query) throws JsonQueryException {
				return JsonQuery.compile(query);
			}
		};
		return CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).maximumSize(1000).build(loader);
	}
}

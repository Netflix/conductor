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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import com.netflix.conductor.common.metadata.events.EventHandler.*;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

	private Injector injector;

	@Inject
	public ActionProcessor(WorkflowExecutor executor, MetadataService metadata, Injector injector) {
		this.executor = executor;
		this.metadata = metadata;
		this.injector = injector;
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
			case update_task:
				op = updateTask(action, jsonObj, event, messageId);
				return op;
			case find_update:
				op = find_update(action, jsonObj, event, messageId);
				return op;
			case java_action:
				op = java_action(action, jsonObj, event, messageId);
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

	private Map<String, Object> updateTask(Action action, Object payload, String event, String messageId) throws Exception {
		UpdateTask updateTask = action.getUpdate_task();
		Map<String, Object> op = new HashMap<>();
		try {
			String workflowId = ScriptEvaluator.evalJq(updateTask.getWorkflowId(), payload);
			if (StringUtils.isEmpty(workflowId))
				throw new RuntimeException("workflowId evaluating is empty");

			String taskId = ScriptEvaluator.evalJq(updateTask.getTaskId(), payload);
			if (StringUtils.isEmpty(taskId))
				throw new RuntimeException("taskId evaluating is empty");

			String status = ScriptEvaluator.evalJq(updateTask.getStatus(), payload);
			if (StringUtils.isEmpty(status))
				throw new RuntimeException("status evaluating is empty");

			String failedReason = null;
			if (StringUtils.isNotEmpty(updateTask.getFailedReason())) {
				failedReason = ScriptEvaluator.evalJq(updateTask.getFailedReason(), payload);
			}

			Status taskStatus;
			// If mapping exists - take the task status from mapping
			if (MapUtils.isNotEmpty(updateTask.getStatuses())) {
				taskStatus = getTaskStatus(updateTask.getStatuses().get(status));
			} else {
				taskStatus = getTaskStatus(status);
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

			targetTask.getOutputData().put("conductor.event.name", event);
			targetTask.getOutputData().put("conductor.event.payload", payload);
			targetTask.getOutputData().put("conductor.event.messageId", messageId);
			if (updateTask.getOutput() != null) {
				op.putAll(updateTask.getOutput());
				targetTask.getOutputData().putAll(updateTask.getOutput());
			}
			targetTask.setStatus(taskStatus);

			// Set the reason if task failed. It should be provided in the event
			if (Task.Status.FAILED.equals(taskStatus)) {
				targetTask.setReasonForIncompletion(failedReason);
			}

			// Create task update wrapper and reset timer if in-progress
			TaskResult taskResult = new TaskResult(targetTask);
			if (Task.Status.IN_PROGRESS.equals(taskStatus) && updateTask.getResetStartTime()) {
				taskResult.setResetStartTime(true);
			}

			// Let's update task
			executor.updateTask(taskResult);

			// output data
			op.put("conductor.event.name", event);
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", messageId);
		} catch (Exception e) {
			logger.error("updateTask: failed with " + e.getMessage() + " for action=" + updateTask + ", payload=" + payload, e);
			op.put("error", e.getMessage());
			op.put("action", updateTask);
			op.put("conductor.event.name", event);
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", messageId);
		}

		return op;
	}

	private Map<String, Object> find_update(Action action, Object payload, String event, String messageId) {
		FindUpdate findUpdate = action.getFind_update();
		Map<String, Object> op = new HashMap<>();
		try {
			// Name of the workflow ot be looked for. Performance consideration.
			String workflowName = findUpdate.getWorkflowName();
			if (StringUtils.isEmpty(workflowName))
				throw new RuntimeException("workflowName is empty");

			Map<String, String> inputParameters = findUpdate.getInputParameters();
			if (MapUtils.isEmpty(inputParameters))
				throw new RuntimeException("inputParameters is empty");

			// Convert map value field=expression to the map of field=value
			inputParameters = inputParameters.entrySet().stream().map(entry -> {
				String fieldName = entry.getKey();
				String expression = entry.getValue();
				if (StringUtils.isEmpty(expression))
					throw new RuntimeException(fieldName + " expression is empty");

				String fieldValue;
				try {
					fieldValue = ScriptEvaluator.evalJq(expression, payload);
				} catch (Exception e) {
					throw new RuntimeException(fieldName + " evaluating failed with " + e.getMessage(), e);
				}
				if (StringUtils.isEmpty(fieldValue))
					throw new RuntimeException(fieldName + " evaluating is empty");

				return new HashMap.SimpleEntry<>(fieldName, fieldValue);
			}).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

			// Task status is completed by default. It either can be a constant or expression
			Status taskStatus = Status.COMPLETED;
			if (StringUtils.isNotEmpty(findUpdate.getStatus())) {
				// Get an evaluating which might result in error or empty response
				String status = ScriptEvaluator.evalJq(findUpdate.getStatus(), payload);
				if (StringUtils.isEmpty(status))
					throw new RuntimeException("status evaluating is empty");

				// If mapping exists - take the task status from mapping
				if (MapUtils.isNotEmpty(findUpdate.getStatuses())) {
					status = findUpdate.getStatuses().get(status);
					taskStatus = getTaskStatus(status);
				} else {
					taskStatus = getTaskStatus(status);
				}
			}

			// Working with running workflows only.
			for (Workflow workflow : executor.getRunningWorkflows(workflowName)) {
				// Move on if workflow completed/failed hence no need to update
				if (workflow.getStatus().isTerminal()) {
					continue;
				}

				// Go over all tasks in workflow
				for (Task task : workflow.getTasks()) {
					// Skip not in progress tasks
					if (!task.getStatus().equals(Status.IN_PROGRESS)) {
						continue;
					}

					// Skip all except wait
					if (!task.getTaskType().equalsIgnoreCase(Wait.NAME)) {
						continue;
					}

					// Skip empty tasks
					Map<String, Object> inputData = task.getInputData();
					if (MapUtils.isEmpty(inputData)) {
						continue;
					}

					// Skip task if it does not have ALL keys in the input parameters
					boolean anyMissed = inputParameters.keySet().stream().anyMatch(item -> !inputData.containsKey(item));
					if (anyMissed) {
						continue;
					}

					// Skip if values do not match
					boolean anyNotEqual = inputParameters.entrySet().stream().anyMatch(entry -> {
						String value = inputData.get(entry.getKey()).toString();
						return !entry.getValue().equalsIgnoreCase(value);
					});
					if (anyNotEqual) {
						continue;
					}

					// Otherwise update the task as we found it
					logger.info("find_update. Updating task " + task + " in " + workflow.getWorkflowId() + " workflow");
					task.getOutputData().put("conductor.event.name", event);
					task.getOutputData().put("conductor.event.payload", payload);
					task.getOutputData().put("conductor.event.messageId", messageId);
					task.setStatus(taskStatus);

					// Set the reason if task failed. It should be provided in the event
					if (Task.Status.FAILED.equals(taskStatus)) {
						String failedReason = null;
						if (StringUtils.isNotEmpty(findUpdate.getFailedReason())) {
							failedReason = ScriptEvaluator.evalJq(findUpdate.getFailedReason(), payload);
						}
						task.setReasonForIncompletion(failedReason);
					}

					// Create task update wrapper and update the task
					TaskResult taskResult = new TaskResult(task);
					executor.updateTask(taskResult);

					// output data
					op.put("conductor.event.name", event);
					op.put("conductor.event.payload", payload);
					op.put("conductor.event.messageId", messageId);
				}
			}
		} catch (Exception e) {
			logger.error("find_update: failed with " + e.getMessage() + " for action=" + findUpdate + ", payload=" + payload, e);
			op.put("error", e.getMessage());
			op.put("action", findUpdate);
			op.put("conductor.event.name", event);
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", messageId);
		}

		return op;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> java_action(Action action, Object payload, String event, String messageId) {
		JavaAction params = action.getJava_action();
		Map<String, Object> op = new HashMap<>();
		try {
			if (StringUtils.isEmpty(params.getClassName())) {
				throw new RuntimeException("No className provided in the action");
			}

			Class clazz = Class.forName(params.getClassName());
			Object object = injector.getInstance(clazz);
			JavaEventAction instance = (JavaEventAction)object;
			op = instance.handle(action, payload, event, messageId);

			op.put("conductor.event.name", event);
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", messageId);
		} catch (Exception e) {
			logger.error("javaAction: failed with " + e.getMessage() + " for action=" + params + ", payload=" + payload, e);
			op.put("error", e.getMessage());
			op.put("action", params);
			op.put("conductor.event.name", event);
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", messageId);
		}

		return op;
	}

	private Status getTaskStatus(String status) {
		try {
			return Status.valueOf(status.toUpperCase());
		} catch (Exception ex) {
			logger.error("getTaskStatus: failed with " + ex.getMessage() + " for " + status);
		}
		return null;
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
}

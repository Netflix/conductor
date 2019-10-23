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
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.*;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.TaskUtils;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	public Map<String, Object> execute(Action action, String payload, EventExecution ee) throws Exception {

		Object jsonObj = om.readValue(payload, Object.class);
		if (action.isExpandInlineJSON()) {
			jsonObj = expand(jsonObj);
		}

		switch (action.getAction()) {
			case start_workflow:
				Map<String, Object> op = startWorkflow(action, jsonObj, ee.getEvent(), ee.getMessageId());
				return op;
			case complete_task:
				op = completeTask(action, jsonObj, action.getComplete_task(), Status.COMPLETED, ee.getEvent(), ee.getMessageId());
				return op;
			case fail_task:
				op = completeTask(action, jsonObj, action.getFail_task(), Status.FAILED, ee.getEvent(), ee.getMessageId());
				return op;
			case update_task:
				op = updateTask(action, jsonObj, ee.getEvent(), ee.getMessageId());
				return op;
			case find_update:
				op = find_update(action, jsonObj, ee);
				return op;
			case java_action:
				op = java_action(action, jsonObj, ee);
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
		input.putAll(new HashMap<>(taskDetails.getOutput()));

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
			replaced.put("conductor.event.success", true);
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
			Map<String, Object> inputParams = new HashMap<>(params.getInput());
			Map<String, Object> workflowInput = pu.replace(inputParams, payload);
			workflowInput.put("conductor.event.messageId", messageId);
			workflowInput.put("conductor.event.name", event);

			String id = executor.startWorkflow(def.getName(), def.getVersion(), workflowInput,
				params.getCorrelationId(), null, null, event, null,
				null, null, null, null, null, false);

			op.put("workflowId", id);
			op.put("conductor.event.success", true);

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
				throw new RuntimeException("Unable to determine workflowId. Check mapping and payload");

			String taskId;
			if (StringUtils.isNotEmpty(updateTask.getTaskId())) {
				taskId = ScriptEvaluator.evalJq(updateTask.getTaskId(), payload);
			} else {
				taskId = null;
			}

			String taskRef;
			if (StringUtils.isNotEmpty(updateTask.getTaskRef())) {
				taskRef = ScriptEvaluator.evalJq(updateTask.getTaskRef(), payload);
			} else {
				taskRef = null;
			}
			if (StringUtils.isEmpty(taskId) && StringUtils.isEmpty(taskRef))
				throw new RuntimeException("Unable to determine taskId/taskRef " + taskId + "/" + taskRef + ". Check mapping and payload");

			String status = ScriptEvaluator.evalJq(updateTask.getStatus(), payload);
			if (StringUtils.isEmpty(status))
				throw new RuntimeException("Unable to determine status. Check mapping and payload");

			String failedReason = null;
			if (StringUtils.isNotEmpty(updateTask.getFailedReason())) {
				failedReason = ScriptEvaluator.evalJq(updateTask.getFailedReason(), payload);
			}

			Status taskStatus;
			// If mapping exists - take the task status from mapping
			if (MapUtils.isNotEmpty(updateTask.getStatuses())) {
				taskStatus = TaskUtils.getTaskStatus(updateTask.getStatuses().get(status));
			} else {
				taskStatus = TaskUtils.getTaskStatus(status);
			}
			if (taskStatus == null)
				throw new RuntimeException("Unable to determine task status. Check mapping and payload");

			Workflow workflow = executor.getWorkflow(workflowId, true);
			if (workflow == null)
				throw new RuntimeException("No workflow found with id " + workflowId);
			Task targetTask = null;
			if (StringUtils.isNotEmpty(taskId)) {
				targetTask = workflow.getTasks().stream().filter(item -> taskId.equals(item.getTaskId()))
						.findFirst().orElse(null);
			} else if (StringUtils.isNotEmpty(taskRef)) {
				targetTask = workflow.getTasks().stream().filter(item -> taskRef.equals(item.getReferenceTaskName()))
						.findFirst().orElse(null);
			}

			if (targetTask == null)
				throw new RuntimeException("No task found with id/ref " + taskId + "/" + taskRef + " for workflow " + workflowId);

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
			op.put("conductor.event.success", true);
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

	private Map<String, Object> find_update(Action action, Object payload, EventExecution ee) throws Exception {
		EventHandler.FindUpdate params = action.getFind_update();
		Map<String, Object> op = new HashMap<>();
		try {
			FindUpdateAction findUpdateAction = new FindUpdateAction(executor);
			List<?> ids = findUpdateAction.handle(action, payload, ee);

			op.put("conductor.event.name", ee.getEvent());
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", ee.getMessageId());
			op.put("conductor.event.success", CollectionUtils.isNotEmpty(ids));

		} catch (Exception e) {
			logger.error("find_update: failed with " + e.getMessage() + " for action=" + params + ", payload=" + payload, e);
			op.put("error", e.getMessage());
			op.put("action", params);
			op.put("conductor.event.name", ee.getEvent());
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", ee.getMessageId());
		}

		return op;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> java_action(Action action, Object payload, EventExecution ee) {
		JavaAction params = action.getJava_action();
		Map<String, Object> op = new HashMap<>();
		try {
			if (StringUtils.isEmpty(params.getClassName())) {
				throw new RuntimeException("No className provided in the action");
			}

			Class clazz = Class.forName(params.getClassName());
			Object object = injector.getInstance(clazz);
			JavaEventAction javaEventAction = (JavaEventAction) object;
			List<?> ids = javaEventAction.handle(action, payload, ee);

			op.put("conductor.event.name", ee.getEvent());
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", ee.getMessageId());
			op.put("conductor.event.success", CollectionUtils.isNotEmpty(ids));
		} catch (Exception e) {
			logger.error("javaAction: failed with " + e.getMessage() + " for action=" + params + ", payload=" + payload, e);
			op.put("error", e.getMessage());
			op.put("action", params);
			op.put("conductor.event.name", ee.getEvent());
			op.put("conductor.event.payload", payload);
			op.put("conductor.event.messageId", ee.getMessageId());
		}

		return op;
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

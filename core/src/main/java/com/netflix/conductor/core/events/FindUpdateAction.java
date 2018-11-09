package com.netflix.conductor.core.events;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.utils.TaskUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FindUpdateAction implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(FindUpdateAction.class);
	private WorkflowExecutor executor;

	public FindUpdateAction(WorkflowExecutor executor) {
		this.executor = executor;
	}

	@Override
	public void handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		handleInternal(action, payload, event, messageId);
	}

	public List<String> handleInternal(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		Set<String> output = new HashSet<>();
		EventHandler.FindUpdate findUpdate = action.getFind_update();

		if (MapUtils.isEmpty(findUpdate.getInputParameters()))
			throw new RuntimeException("No inputParameters defined in the action");

		// Convert map value field=expression to the map of field=value
		Map<String, String> inputParameters = ScriptEvaluator.evaluateMap(findUpdate.getInputParameters(), payload);

		// Task status is completed by default. It either can be a constant or expression
		Task.Status taskStatus;
		if (StringUtils.isNotEmpty(findUpdate.getStatus())) {
			// Get an evaluating which might result in error or empty response
			String status = ScriptEvaluator.evalJq(findUpdate.getStatus(), payload);
			if (StringUtils.isEmpty(status))
				throw new RuntimeException("Unable to determine status. Check mapping and payload");

			// If mapping exists - take the task status from mapping
			if (MapUtils.isNotEmpty(findUpdate.getStatuses())) {
				status = findUpdate.getStatuses().get(status);
				taskStatus = TaskUtils.getTaskStatus(status);
			} else {
				taskStatus = TaskUtils.getTaskStatus(status);
			}
		} else {
			taskStatus = Task.Status.COMPLETED;
		}

		String mainWorkflowId;
		if (StringUtils.isNotEmpty(findUpdate.getMainWorkflowId())) {
			mainWorkflowId = ScriptEvaluator.evalJq(findUpdate.getMainWorkflowId(), payload);
			if (StringUtils.isEmpty(mainWorkflowId))
				throw new RuntimeException("Unable to determine mainWorkflowId. Check mapping and payload");
		} else {
			mainWorkflowId = null;
		}

		// Lets find WAIT + IN_PROGRESS tasks directly via edao
		boolean taskNamesDefined = CollectionUtils.isNotEmpty(findUpdate.getTaskRefNames());
		List<Task> tasks = executor.getPendingSystemTasks(Wait.NAME);
		tasks.parallelStream().forEach(task -> {
			try {
				Workflow workflow = executor.getWorkflow(task.getWorkflowInstanceId(), false);
				if (workflow.getStatus().isTerminal()) {
					return;
				}

				if (taskNamesDefined && !findUpdate.getTaskRefNames().contains(task.getReferenceTaskName())) {
					return;
				}

				// Find the root level workflow
				if (StringUtils.isNotEmpty(mainWorkflowId)) {
					String rootWorkflowId = findMainWorkflowId(workflow);
					// Move on if does not match
					if (!mainWorkflowId.equals(rootWorkflowId)) {
						return;
					}
				}

				// Complex match - either legacy mode (compare maps) or the JQ expression against two maps
				boolean matches = matches(task.getInputData(), inputParameters, findUpdate.getExpression());
				if (!matches) {
					return;
				}

				// Otherwise update the task as we found it
				task.setStatus(taskStatus);
				task.getOutputData().put("conductor.event.name", event);
				task.getOutputData().put("conductor.event.payload", payload);
				task.getOutputData().put("conductor.event.messageId", messageId);
				logger.debug("Updating task " + task + ". workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId());

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
				output.add(workflow.getWorkflowId());

			} catch (Exception ex) {
				logger.error("Find update failed for taskId={}, messageId={}, event={}, workflowId={}, correlationId={}, payload={}",
						task.getTaskId(), messageId, event, task.getWorkflowInstanceId(), task.getCorrelationId(), payload, ex);
			}
		});

		return new ArrayList<>(output);
	}

	private String findMainWorkflowId(Workflow workflow) {
		if (workflow == null)
			return null;

		if (StringUtils.isNotEmpty(workflow.getParentWorkflowId())) {
			Workflow parent = executor.getWorkflow(workflow.getParentWorkflowId(), false);
			return findMainWorkflowId(parent);
		} else {
			return workflow.getWorkflowId();
		}
	}

	private boolean matches(Map<String, Object> task, Map<String, String> event, String expression) throws Exception {

		// Use JQ expression
		if (StringUtils.isNotEmpty(expression)) {
			Map<String, Object> map = new HashMap<>();
			map.put("task", task);
			map.put("event", event);

			String result = ScriptEvaluator.evalJq(expression, map);
			return "true".equals(result);
		} else { // Legacy mode
			// Skip empty tasks
			if (MapUtils.isEmpty(task)) {
				return false;
			}

			// Skip task if it does not have ALL keys in the input parameters
			boolean anyMissed = event.keySet().stream().anyMatch(item -> !task.containsKey(item));
			if (anyMissed) {
				return false;
			}

			// Skip if values do not match totally
			boolean anyNotEqual = event.entrySet().stream().anyMatch(entry -> {
				String taskValue = (String)task.get(entry.getKey());
				String msgValue = entry.getValue();
				return !(StringUtils.isNotEmpty(taskValue)
						&& StringUtils.isNotEmpty(msgValue)
						&& msgValue.equals(taskValue));
			});

			// anyNotEqual is true if any of values does not match. false means all match
			return !anyNotEqual;
		}
	}
}

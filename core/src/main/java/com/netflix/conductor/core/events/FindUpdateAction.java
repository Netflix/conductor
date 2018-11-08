package com.netflix.conductor.core.events;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.utils.TaskUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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
		List<String> output = new LinkedList<>();
		EventHandler.FindUpdate findUpdate = action.getFind_update();

		// Name of the workflow ot be looked for. Performance consideration.
		String workflowName = findUpdate.getWorkflowName();
		if (StringUtils.isEmpty(workflowName))
			throw new RuntimeException("Unable to determine workflowName. Check mapping and payload");

		Map<String, String> inputParameters = findUpdate.getInputParameters();
		if (MapUtils.isEmpty(inputParameters))
			throw new RuntimeException("No inputParameters defined in the action");

		// Convert map value field=expression to the map of field=value
		inputParameters = ScriptEvaluator.evaluateMap(inputParameters, payload);

		// Task status is completed by default. It either can be a constant or expression
		Task.Status taskStatus = Task.Status.COMPLETED;
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
		}

		List<Workflow> workflows;
		if (StringUtils.isNotEmpty(findUpdate.getMainWorkflowId())) {
			String workflowId = ScriptEvaluator.evalJq(findUpdate.getMainWorkflowId(), payload);
			if (StringUtils.isEmpty(workflowId))
				throw new RuntimeException("Unable to determine mainWorkflowId. Check mapping and payload");

			Workflow workflow = executor.getWorkflow(workflowId, true);

			// Let's find the sub-workflow
			workflows = findChildSubWorkflow(workflow, workflowName);
		} else {
			workflows = executor.getRunningWorkflows(workflowName);
		}

		// Working with running workflows only.
		for (Workflow workflow : workflows) {
			// Move on if workflow completed/failed hence no need to update
			if (workflow.getStatus().isTerminal()) {
				continue;
			}

			// Go over all tasks in workflow
			for (Task task : workflow.getTasks()) {
				// Skip not in progress tasks
				if (!task.getStatus().equals(Task.Status.IN_PROGRESS)) {
					continue;
				}

				// Skip all except wait
				if (!task.getTaskType().equalsIgnoreCase(Wait.NAME)) {
					continue;
				}

				// Complex match - either legacy mode (compare maps) or the JQ expression against two maps
				boolean matches = matches(task.getInputData(), inputParameters, findUpdate.getExpression());
				if (!matches) {
					continue;
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
			}
		}

		return output;
	}

	private List<Workflow> findChildSubWorkflow(Workflow parent, String workflowName) {
		// Let's find the sub-workflow in the current parent
		List<Workflow> result = parent.getTasks().stream()
				.filter(task -> SubWorkflow.NAME.equals(task.getTaskType()))
				.filter(task -> task.getInputData().get("subWorkflowId") != null)
				.filter(task -> workflowName.equals(task.getInputData().get("subWorkflowName")))
				.map(task -> executor.getWorkflow(task.getInputData().get("subWorkflowId").toString(), true))
				.collect(Collectors.toList());
		// Exit if found anything ?
		if (CollectionUtils.isNotEmpty(result)) {
			return result;
		}
		// If not found, then consider each child sub-workflow as parent recursively
		return parent.getTasks().stream()
				.filter(task -> SubWorkflow.NAME.equals(task.getTaskType()))
				.filter(task -> task.getInputData().get("subWorkflowId") != null)
				.map(task -> executor.getWorkflow(task.getInputData().get("subWorkflowId").toString(), true))
				.map(workflow -> findChildSubWorkflow(workflow, workflowName))
				.flatMap(Collection::stream)
				.collect(Collectors.toList());
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

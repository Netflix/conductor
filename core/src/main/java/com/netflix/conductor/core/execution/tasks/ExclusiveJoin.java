package com.netflix.conductor.core.execution.tasks;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

public class ExclusiveJoin extends WorkflowSystemTask {
	private static Logger logger = LoggerFactory.getLogger(ExclusiveJoin.class);
	private static final String NAME = "EXCLUSIVE_JOIN";
	private static final String DEFAULT_EXCLUSIVE_JOIN_TASKS = "defaultExclusiveJoinTask";

	public ExclusiveJoin() {
		super(NAME);
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) {

		boolean foundExlusiveJoinOnTask = false;
		boolean hasFailures = false;
		boolean useWorkflowInput = false;
		StringBuilder failureReason = new StringBuilder();
		Task.Status taskStatus = null;
		List<String> joinOn = (List<String>) task.getInputData().get("joinOn");
		Task exclusiveTask = null;
		for (String joinOnRef : joinOn) {
			logger.info(" Exclusive Join On Task {} ", joinOnRef);
			exclusiveTask = workflow.getTaskByRefName(joinOnRef);
			if (exclusiveTask == null || exclusiveTask.getStatus() == Task.Status.SKIPPED) {
				continue;
			}
			taskStatus = exclusiveTask.getStatus();
			foundExlusiveJoinOnTask = taskStatus.isTerminal();
			hasFailures = !taskStatus.isSuccessful();
			if (hasFailures) {
				failureReason.append(exclusiveTask.getReasonForIncompletion()).append(" ");
			}

			break;
		}

		if (!foundExlusiveJoinOnTask) {
			List<String> defaultExclusiveJoinTasks = (List<String>) task.getInputData()
					.get(DEFAULT_EXCLUSIVE_JOIN_TASKS);
			logger.info(
					"Could not perform exclusive on Join Task(s). Performing now on default exclusive join task(s) {}",
					defaultExclusiveJoinTasks);
			if (defaultExclusiveJoinTasks != null && !defaultExclusiveJoinTasks.isEmpty()) {
				for (String defaultExclusiveJoinTask : defaultExclusiveJoinTasks) {
					// Pick the first task that we should join on and break.
					exclusiveTask = workflow.getTaskByRefName(defaultExclusiveJoinTask);
					if (exclusiveTask != null) {
						taskStatus = exclusiveTask.getStatus();
						foundExlusiveJoinOnTask = taskStatus.isTerminal();
						hasFailures = !taskStatus.isSuccessful();
						if (hasFailures) {
							failureReason.append(exclusiveTask.getReasonForIncompletion()).append(" ");
						}
						break;
					} else {
						logger.debug(" defaultExclusiveJoinTask {} is not found/executed ", defaultExclusiveJoinTask);
					}
				}
			} else {
				useWorkflowInput = true;
			}
		}

		logger.debug("Status of flags: foundExlusiveJoinOnTask: {}, hasFailures {}, useWorkflowInput {}",
				foundExlusiveJoinOnTask, hasFailures, useWorkflowInput);
		if (foundExlusiveJoinOnTask || hasFailures || useWorkflowInput) {
			if (hasFailures) {
				task.setReasonForIncompletion(failureReason.toString());
				task.setStatus(Task.Status.FAILED);
			} else {
				task.setOutputData((useWorkflowInput) ? workflow.getInput() : exclusiveTask.getOutputData());
				task.setStatus(Task.Status.COMPLETED);
			}
			logger.debug("Task status is: {}", task.getStatus());
			return true;
		}
		return false;
	}
}
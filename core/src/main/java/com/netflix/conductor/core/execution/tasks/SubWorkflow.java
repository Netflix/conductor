/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Viren
 *
 */
public class SubWorkflow extends WorkflowSystemTask {

	private static final Logger logger = LoggerFactory.getLogger(SubWorkflow.class);
	private static final String SUPPRESS_RESTART_PARAMETER = "suppressRestart";
	private static final String RESTARTED = "restartCount";
	private static final String RESTART_ON = "restartOn";
	public static final String NAME = "SUB_WORKFLOW";

	public SubWorkflow() {
		super(NAME);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {

		Map<String, Object> input = task.getInputData();
		String name = input.get("subWorkflowName").toString();
		int version = (int) input.get("subWorkflowVersion");
		Map<String, Object> wfInput = (Map<String, Object>) input.get("workflowInput");
		if (wfInput == null || wfInput.isEmpty()) {
			wfInput = input;
		}
		String correlationId = workflow.getCorrelationId();

		try {

			String subWorkflowId = provider.startWorkflow(name, version, wfInput, correlationId, workflow.getWorkflowId(), task.getTaskId(), null, workflow.getTaskToDomain(), workflow.getWorkflowIds());
			task.getOutputData().put("subWorkflowId", subWorkflowId);
			task.getInputData().put("subWorkflowId", subWorkflowId);
			task.setStatus(Status.IN_PROGRESS);

		} catch (Exception e) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
			logger.error(e.getMessage(), e);
		}
	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {
		String workflowId = (String) task.getOutputData().get("subWorkflowId");
		if (workflowId == null) {
			workflowId = (String) task.getInputData().get("subWorkflowId");    //Backward compatibility
		}

		if (StringUtils.isEmpty(workflowId)) {
			return false;
		}

		Workflow subWorkflow = provider.getWorkflow(workflowId, false);
		WorkflowStatus subWorkflowStatus = subWorkflow.getStatus();
		if (!subWorkflowStatus.isTerminal()) {
			return false;
		}

		SubWorkflowParams param = task.getWorkflowTask().getSubWorkflowParam();
		if (subWorkflowStatus == WorkflowStatus.RESET) {
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " has been reset");
			return handleRestart(subWorkflow, task, param, provider);
		} else if (subWorkflowStatus.isSuccessful()) {
			task.setStatus(Status.COMPLETED);
		} else if (subWorkflowStatus == WorkflowStatus.CANCELLED) {
			task.setStatus(Status.CANCELED);
			task.setReasonForIncompletion("Sub-workflow " + task.getReferenceTaskName() + " has been cancelled");
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		} else if (subWorkflowStatus == WorkflowStatus.TERMINATED) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion("Sub-workflow " + task.getReferenceTaskName() + " has been terminated");
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		} else if (isSuppressRestart(subWorkflow)) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(subWorkflow.getReasonForIncompletion());
			task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		} else {
			// Note: StandbyOnFail and RestartOnFail are Boolean objects and not primitives
			if (BooleanUtils.isTrue(param.isStandbyOnFail())) {

				// No restart required for the sub-workflow
				if (BooleanUtils.isNotTrue(param.isRestartOnFail())) {
					return false;
				}

				return handleRestart(subWorkflow, task, param, provider);

			} else {
				task.setStatus(Status.FAILED);
				task.setReasonForIncompletion(subWorkflow.getReasonForIncompletion());
				task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
			}
		}
		if (task.getStatus() == Status.COMPLETED) {
			Map<String, Object> output = new HashMap<>(subWorkflow.getOutput());
			output.remove("subWorkflowId"); // We should remove subWorkflowId and not propagate back to parent task
			task.getOutputData().putAll(output);
		}
		return true;
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {
		String workflowId = (String) task.getOutputData().get("subWorkflowId");
		if (workflowId == null) {
			workflowId = (String) task.getInputData().get("subWorkflowId");    //Backward compatibility
		}

		if (StringUtils.isEmpty(workflowId)) {
			return;
		}
		Workflow subWorkflow = provider.getWorkflow(workflowId, true);
		if (workflow.getStatus() == WorkflowStatus.CANCELLED) {
			subWorkflow.setStatus(WorkflowStatus.CANCELLED);
			provider.cancelWorkflow(subWorkflow, "Parent workflow has been cancelled", true);
		} else if (workflow.getStatus() == WorkflowStatus.FAILED) {
			subWorkflow.setStatus(WorkflowStatus.FAILED);
			provider.terminateWorkflow(subWorkflow, "Parent workflow has been failed", null, null, true);
		} else {
			subWorkflow.setStatus(WorkflowStatus.TERMINATED);
			provider.terminateWorkflow(subWorkflow, "Parent workflow has been terminated with status " + workflow.getStatus(), null, null, true);
		}
	}

	@Override
	public boolean isAsync() {
		return false;
	}

	private boolean isSuppressRestart(Workflow subWorkflow) {
		Object obj = subWorkflow.getOutput().get(SUPPRESS_RESTART_PARAMETER);
		if (obj instanceof Boolean) {
			return (boolean) obj;
		} else if (obj instanceof String) {
			return Boolean.parseBoolean((String) obj);
		}
		return false;
	}

	private boolean handleRestart(Workflow subWorkflow, Task task, SubWorkflowParams param, WorkflowExecutor provider) {
		Integer restarted = (Integer) task.getOutputData().get(RESTARTED);
		if (restarted == null) {
			restarted = 0;
		}

		Integer restartsAllowed = param.getRestartCount();
		if (restartsAllowed != null && restartsAllowed >= 0 && restarted >= restartsAllowed) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion("Number of restart attempts reached configured value");
			return true;
		}

		logger.debug("Time to restart the sub-workflow " + subWorkflow.getWorkflowId());
		restarted++;
		task.getOutputData().put(RESTARTED, restarted);
		try {
			provider.rewind(subWorkflow.getWorkflowId(), subWorkflow.getCorrelationId());
		} catch (Exception ex) {
			// Do nothing due to conflict situation when this task already been taken care of
			if (ex.getMessage().contains("Workflow is still running")) {
				return false;
			}

			logger.error("Unable to restart the sub-workflow " + subWorkflow.getWorkflowId() +
					", correlationId=" + subWorkflow.getCorrelationId() + " due to " + ex.getMessage(), ex);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(ex.getMessage());

			subWorkflow.setStatus(WorkflowStatus.FAILED);
			subWorkflow.setReasonForIncompletion(ex.getMessage());
			try {
				provider.terminateWorkflow(subWorkflow, ex.getMessage(), null, null, true);
			} catch (Exception ignore) {
			}
		}
		return true;
	}

}

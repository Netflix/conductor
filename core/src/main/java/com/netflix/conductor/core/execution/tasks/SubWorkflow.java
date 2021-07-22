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
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.defaultIfEmpty;

/**
 * @author Viren
 *
 */
public class SubWorkflow extends WorkflowSystemTask {

	private static final Logger logger = LoggerFactory.getLogger(SubWorkflow.class);
	private static final String SUPPRESS_RESTART_PARAMETER = "suppressRestart";
	public static final String NAME = "SUB_WORKFLOW";
	private final boolean async;

	@Inject
	public SubWorkflow(Configuration config) {
		super(NAME);
		async = Boolean.parseBoolean(config.getProperty("workflow.system.task.subworkflow.async", "true"));
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

			String subWorkflowId = provider.startWorkflow(name, version, wfInput, correlationId,
				workflow.getWorkflowId(), task.getTaskId(), null,
				workflow.getTaskToDomain(), workflow.getWorkflowIds(),
				workflow.getAuthorization(), workflow.getContextToken(),
				workflow.getContextUser(), workflow.getTraceId(), async,
				workflow.getJobPriority());

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
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " seems still running");
			return false;
		}

		SubWorkflowParams param = task.getWorkflowTask().getSubWorkflowParam();
		if (subWorkflowStatus == WorkflowStatus.RESET) {
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " has been reset");
			return handleRestart(subWorkflow, task, param, provider);
		} else if (subWorkflowStatus.isSuccessful()) {
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " is successful (" + subWorkflowStatus.name() + ")");
			task.setStatus(Status.COMPLETED);
			task.setReasonForIncompletion(null);
		} else if (subWorkflowStatus == WorkflowStatus.CANCELLED) {
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " has been cancelled");
			task.setStatus(Status.CANCELED);
			task.setReasonForIncompletion(defaultIfEmpty(subWorkflow.getReasonForIncompletion(), "Sub-workflow " + task.getReferenceTaskName() + " has been cancelled"));
			task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
			task.getOutputData().put("cancelledBy", subWorkflow.getCancelledBy());
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		} else if (subWorkflowStatus == WorkflowStatus.TERMINATED) {
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " has been terminated");
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(defaultIfEmpty(subWorkflow.getReasonForIncompletion(), "Sub-workflow " + task.getReferenceTaskName() + " has been terminated"));
			task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		} else if (isSuppressRestart(subWorkflow)) {
			logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " has been failed (suppress restart mode)");
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(subWorkflow.getReasonForIncompletion());
			task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		} else {
			// Note: StandbyOnFail and RestartOnFail are Boolean objects and not primitives
			if (BooleanUtils.isTrue(param.isStandbyOnFail())) {

				// Check restart first
				boolean restartOnFail = BooleanUtils.isTrue(param.isRestartOnFail());
				if (restartOnFail) {
					return handleRestart(subWorkflow, task, param, provider);
				}

				boolean rerunOnFail = param.getRerunWorkflow() != null && StringUtils.isNotEmpty(param.getRerunWorkflow().getName());
				if (rerunOnFail) {
					return handleRerun(workflow, subWorkflow, task, param, provider);
				}

				return false;
			} else {
				logger.debug("The sub-workflow " + subWorkflow.getWorkflowId() + " has been failed (no stand by on fail)");
				task.setStatus(Status.FAILED);
				task.setReasonForIncompletion(subWorkflow.getReasonForIncompletion());
				task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
			}
		}
		if (task.getStatus() == Status.COMPLETED) {
			Map<String, Object> output = new HashMap<>(subWorkflow.getOutput());
			output.remove("subWorkflowId"); // We should remove subWorkflowId and not propagate back to parent task
			output.remove("rerunWorkflowId"); // We should remove rerunWorkflowId and not propagate back to parent task
			task.getOutputData().putAll(output);
		}
		return true;
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {
		String rerunWorkflowId = (String) task.getOutputData().get("rerunWorkflowId");
		if (StringUtils.isNotEmpty(rerunWorkflowId)) {
			try {
				Workflow rerunWorkflow = provider.getWorkflow(rerunWorkflowId, true);

				if (workflow.getStatus() == WorkflowStatus.COMPLETED) {
					provider.forceCompleteWorkflow(rerunWorkflowId, "Parent workflow force completed");
				} else if (workflow.getStatus() == WorkflowStatus.CANCELLED) {
					rerunWorkflow.setStatus(WorkflowStatus.CANCELLED);
					rerunWorkflow.setCancelledBy(workflow.getCancelledBy());
					provider.cancelWorkflow(rerunWorkflow, defaultIfEmpty(workflow.getReasonForIncompletion(), "Parent workflow has been cancelled"));
				} else {
					provider.terminateWorkflow(rerunWorkflowId, "The sub-workflow cancellation requested");
				}
			} catch (Exception ex) {
				logger.warn("Rerun workflow termination failed " + ex.getMessage() + " for " + rerunWorkflowId);
			}
		}

		String workflowId = (String) task.getOutputData().get("subWorkflowId");
		if (workflowId == null) {
			workflowId = (String) task.getInputData().get("subWorkflowId");    //Backward compatibility
		}

		if (StringUtils.isEmpty(workflowId)) {
			return;
		}
		Workflow subWorkflow = provider.getWorkflow(workflowId, true);
		if (subWorkflow == null) {
			logger.debug("No workflow found with id " + workflowId + " while cancelling " + task);
			return;
		}

		logger.debug("Workflow cancellation requested. workflowId=" + subWorkflow.getWorkflowId()
			+ ", correlationId=" + subWorkflow.getCorrelationId()
			+ ", traceId=" + workflow.getTraceId()
			+ ", contextUser=" + subWorkflow.getContextUser()
			+ ", parentWorkflowId=" + subWorkflow.getParentWorkflowId());

		if (workflow.getStatus() == WorkflowStatus.COMPLETED) {
			provider.forceCompleteWorkflow(workflowId, "Parent workflow force completed");
		} else if (workflow.getStatus() == WorkflowStatus.CANCELLED) {
			subWorkflow.setStatus(WorkflowStatus.CANCELLED);
			subWorkflow.setCancelledBy(workflow.getCancelledBy());
			provider.cancelWorkflow(subWorkflow, defaultIfEmpty(workflow.getReasonForIncompletion(), "Parent workflow has been cancelled"));
		} else if (workflow.getStatus() == WorkflowStatus.FAILED) {
			subWorkflow.setStatus(WorkflowStatus.FAILED);
			provider.terminateWorkflow(subWorkflow, defaultIfEmpty(workflow.getReasonForIncompletion(), "Parent workflow has been failed"), null, null);
		} else {
			subWorkflow.setStatus(WorkflowStatus.TERMINATED);
			provider.terminateWorkflow(subWorkflow, defaultIfEmpty(workflow.getReasonForIncompletion(), "Parent workflow has been terminated with status " + workflow.getStatus()), null, null);
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
		logger.debug("handleRestart invoked for sub-workflow=" + subWorkflow.getWorkflowId());

		Integer restarted = subWorkflow.getRestartCount();

		Integer restartsAllowed = param.getRestartCount();
		if (restartsAllowed != null && restartsAllowed >= 0 && restarted >= restartsAllowed) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion("Number of restart attempts reached configured value");
			return true;
		}

		logger.debug("Time to restart the sub-workflow " + subWorkflow.getWorkflowId());
		try {
			// restart count will be increased in the rewind method
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
				provider.terminateWorkflow(subWorkflow, ex.getMessage(), null, null);
			} catch (Exception ignore) {
			}
		}
		return true;
	}

	private boolean handleRerun(Workflow workflow, Workflow subWorkflow, Task task, SubWorkflowParams param, WorkflowExecutor provider) {
		logger.trace("handleRerun invoked for sub-workflow=" + subWorkflow.getWorkflowId()
			+ ", correlationId=" + subWorkflow.getCorrelationId()
			+ ", traceId=" + subWorkflow.getTraceId()
			+ ", contextUser=" + subWorkflow.getContextUser());

		String rerunWorkflowId = (String) task.getOutputData().get("rerunWorkflowId");
		Workflow rerunWorkflow = null;
		if (StringUtils.isNotEmpty(rerunWorkflowId)) {
			rerunWorkflow = provider.getWorkflow(rerunWorkflowId, false);
			if (rerunWorkflow == null) {
				logger.warn("Cannot found rerun workflow by id " + rerunWorkflowId);
				return false;
			}

			// Exit if still in progress
			if (!rerunWorkflow.getStatus().isTerminal()) {
				return false;
			}
		}

		// Rerun workflow input
		Map<String, Object> wfInput = getRerunInput(workflow, subWorkflow, rerunWorkflow);

		// Add latest rerun output to the map
		if (rerunWorkflow != null) {
			if (MapUtils.isEmpty(param.getRerunWorkflow().getConditions()))
				throw new IllegalArgumentException("No defined rules in rerun options for " + task.getReferenceTaskName() + " sub-workflow task");

			Map<String, Object> evaluatedMap = ScriptEvaluator.evaluateMap(param.getRerunWorkflow().getConditions(), wfInput);
			logger.trace("Rerun evaluated rules " + evaluatedMap);

			boolean allowRerun = evaluatedMap.entrySet().stream().allMatch(entry -> {
				logger.trace("Rerun rule: " + entry.getKey() + "=" + entry.getValue() + "/" + entry.getValue().getClass().getName());

				if (entry.getValue() == null) {
					return false;
				}

				return "true".equalsIgnoreCase(entry.getValue().toString());
			});

			if (!allowRerun) {
				logger.debug("No more rerun allowed for the sub-workflow=" + subWorkflow.getWorkflowId()
					+ ", correlationId=" + subWorkflow.getCorrelationId()
					+ ", traceId=" + subWorkflow.getTraceId()
					+ ", contextUser=" + subWorkflow.getContextUser());

				task.setStatus(Status.FAILED);
				task.setReasonForIncompletion(subWorkflow.getReasonForIncompletion());
				task.getOutputData().put("originalFailedTask", subWorkflow.getOutput().get("originalFailedTask"));
				return true;
			}
		}

		try {

			logger.debug("Time to rerun the sub-workflow=" + subWorkflow.getWorkflowId()
				+ ", correlationId=" + subWorkflow.getCorrelationId()
				+ ", traceId=" + subWorkflow.getTraceId()
				+ ", contextUser=" + subWorkflow.getContextUser());

			String name = param.getRerunWorkflow().getName();
			int version = (int) ObjectUtils.defaultIfNull(param.getRerunWorkflow().getVersion(), 1);

			rerunWorkflowId = provider.startWorkflow(name, version, wfInput, workflow.getCorrelationId(),
				workflow.getWorkflowId(), task.getTaskId(), null,
				workflow.getTaskToDomain(), workflow.getWorkflowIds(),
				workflow.getAuthorization(), workflow.getContextToken(),
				workflow.getContextUser(), workflow.getTraceId(), true,
				workflow.getJobPriority());

			task.getOutputData().put("rerunWorkflowId", rerunWorkflowId);

		} catch (Exception ex) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(ex.getMessage());

			logger.error("Start rerun workflow failed " + ex.getMessage() + " for sub-workflow=" + subWorkflow.getWorkflowId()
				+ ", correlationId=" + subWorkflow.getCorrelationId()
				+ ", traceId=" + subWorkflow.getTraceId()
				+ ", contextUser=" + subWorkflow.getContextUser(), ex);
		}

		return true;
	}

	private Map<String, Object> getRerunInput(Workflow workflow, Workflow subWorkflow, Workflow rerunWorkflow) {
		Map<String, Object> result = new HashMap<>();

		// The main workflow details
		result.put("workflowInput", workflow.getInput());
		result.put("workflowId", workflow.getWorkflowId());
		result.put("workflowType", workflow.getWorkflowType());
		result.put("workflowVersion", workflow.getVersion());
		result.put("correlationId", workflow.getCorrelationId());
		result.put("failureStatus", workflow.getStatus().toString());
		result.put("reason", subWorkflow.getReasonForIncompletion());

		// Failed sub-workflow details
		Map<String, Object> failedSubWorkflow = new HashMap<>();
		failedSubWorkflow.put("workflowId", subWorkflow.getWorkflowId());
		failedSubWorkflow.put("failureStatus", subWorkflow.getStatus().toString());
		result.put("subWorkflow", failedSubWorkflow);

		if (rerunWorkflow != null) {
			result.put("rerunOutput", rerunWorkflow.getOutput());
		}

		return result;
	}
}

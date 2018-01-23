/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.DeciderService.DeciderOutcome;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Viren Workflow services provider interface
 */
@Trace
public class WorkflowExecutor {
	private static final String BEARER = "Bearer ";

	public enum StartEndState {
		start, end
	}

	private static Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);

	private MetadataDAO metadata;

	private ExecutionDAO edao;

	private QueueDAO queue;

	private DeciderService decider;

	private Configuration config;

	private AuthManager auth;

	public static final String deciderQueue = "_deciderQueue";

	private int activeWorkerLastPollnSecs;

	private boolean validateAuth;

	@Inject
	public WorkflowExecutor(MetadataDAO metadata, ExecutionDAO edao, QueueDAO queue, ObjectMapper om, AuthManager auth, Configuration config) {
		this.metadata = metadata;
		this.edao = edao;
		this.queue = queue;
		this.config = config;
		this.auth = auth;
		activeWorkerLastPollnSecs = config.getIntProperty("tasks.active.worker.lastpoll", 10);
		this.decider = new DeciderService(metadata, om);
		this.validateAuth = Boolean.parseBoolean(config.getProperty("workflow.auth.validate", "false"));
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input) throws Exception {
		return startWorkflow(name, version, correlationId, input, null);
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input, String event) throws Exception {
		return startWorkflow(name, version, input, correlationId, null, null, event);
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input, String event, Map<String, String> taskToDomain) throws Exception {
		return startWorkflow(name, version, input, correlationId, null, null, event, taskToDomain, null);
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input, String event, Map<String, String> taskToDomain, Map<String, Object> headers) throws Exception {
		return startWorkflow(name, version, input, correlationId, null, null, event, taskToDomain, headers);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event) throws Exception {
		return startWorkflow(name, version, input, correlationId, parentWorkflowId, parentWorkflowTaskId, event, null, null);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, Map<String, String> taskToDomain) throws Exception {
		return startWorkflow(name, version, input, correlationId, parentWorkflowId, parentWorkflowTaskId, event, taskToDomain, null);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, Map<String, String> taskToDomain, Map<String, Object> headers) throws Exception {

		try {

			if(input == null){
				throw new ApplicationException(Code.INVALID_INPUT, "NULL input passed when starting workflow");
			}

			WorkflowDef exists = metadata.get(name, version);
			if (exists == null) {
				throw new ApplicationException(Code.NOT_FOUND, "No such workflow defined. name=" + name + ", version=" + version);
			}

			// Input validation required
			if (exists.getInputValidation() != null && !exists.getInputValidation().isEmpty()) {
				validateWorkflowInput(exists, input);
			}

			// Auth validation if requested and only when rules are defined in workflow
			if (this.validateAuth && MapUtils.isNotEmpty(exists.getAuthValidation())) {
				validateAuth(exists, headers);
			}

			Set<String> missingTaskDefs = exists.all().stream()
					.filter(wft -> wft.getType().equals(WorkflowTask.Type.SIMPLE.name()))
					.map(wft2 -> wft2.getName()).filter(task -> metadata.getTaskDef(task) == null)
					.collect(Collectors.toSet());
			if(!missingTaskDefs.isEmpty()) {
				throw new ApplicationException(Code.INVALID_INPUT, "Cannot find the task definitions for the following tasks used in workflow: " + missingTaskDefs);
			}
			String workflowId = IDGenerator.generate();

			// Persist the Workflow
			Workflow wf = new Workflow();
			wf.setWorkflowId(workflowId);
			wf.setCorrelationId(correlationId);
			wf.setWorkflowType(name);
			wf.setVersion(version);
			wf.setInput(input);
			wf.setStatus(WorkflowStatus.RUNNING);
			wf.setParentWorkflowId(parentWorkflowId);
			wf.setParentWorkflowTaskId(parentWorkflowTaskId);
			wf.setOwnerApp(WorkflowContext.get().getClientApp());
			wf.setCreateTime(System.currentTimeMillis());
			wf.setUpdatedBy(null);
			wf.setUpdateTime(null);
			wf.setEvent(event);
			wf.setTaskToDomain(taskToDomain);
			wf.setHeaders(headers);
			edao.createWorkflow(wf);

			// send wf start message
			notifyWorkflowStatus(wf, StartEndState.start);

			decide(workflowId);
			logger.info("Workflow has started.Current status=" + wf.getStatus() + ",workflowId=" + wf.getWorkflowId()+",CorrelationId=" + wf.getCorrelationId()+",input="+wf.getInput());
			return workflowId;

		}catch (Exception e) {
			Monitors.recordWorkflowStartError(name);
			throw e;
		}
	}

	public String rerun(RerunWorkflowRequest request) throws Exception {

		Workflow reRunFromWorkflow = edao.getWorkflow(request.getReRunFromWorkflowId());

		String workflowId = IDGenerator.generate();

		// Persist the workflow and task First
		Workflow wf = new Workflow();
		wf.setWorkflowId(workflowId);
		wf.setCorrelationId((request.getCorrelationId() == null) ? reRunFromWorkflow.getCorrelationId() : request.getCorrelationId());
		wf.setWorkflowType(reRunFromWorkflow.getWorkflowType());
		wf.setVersion(reRunFromWorkflow.getVersion());
		wf.setInput((request.getWorkflowInput() == null) ? reRunFromWorkflow.getInput() : request.getWorkflowInput());
		wf.setReRunFromWorkflowId(request.getReRunFromWorkflowId());
		wf.setStatus(WorkflowStatus.RUNNING);
		wf.setOwnerApp(WorkflowContext.get().getClientApp());
		wf.setCreateTime(System.currentTimeMillis());
		wf.setUpdatedBy(null);
		wf.setUpdateTime(null);

		// If the "reRunFromTaskId" is not given in the RerunWorkflowRequest,
		// then the whole
		// workflow has to rerun
		if (request.getReRunFromTaskId() != null) {
			// We need to go thru the workflowDef and create tasks for
			// all tasks before request.getReRunFromTaskId() and marked them
			// skipped
			List<Task> newTasks = new LinkedList<>();
			Map<String, Task> refNameToTask = new HashMap<String, Task>();
			reRunFromWorkflow.getTasks().forEach(task -> refNameToTask.put(task.getReferenceTaskName(), task));
			WorkflowDef wd = metadata.get(reRunFromWorkflow.getWorkflowType(), reRunFromWorkflow.getVersion());
			Iterator<WorkflowTask> it = wd.getTasks().iterator();
			int seq = wf.getTasks().size();
			while (it.hasNext()) {
				WorkflowTask wt = it.next();
				Task previousTask = refNameToTask.get(wt.getTaskReferenceName());
				if (previousTask.getTaskId().equals(request.getReRunFromTaskId())) {
					Task theTask = new Task();
					theTask.setTaskId(IDGenerator.generate());
					theTask.setReferenceTaskName(previousTask.getReferenceTaskName());
					theTask.setInputData((request.getTaskInput() == null) ? previousTask.getInputData() : request.getTaskInput());
					theTask.setWorkflowInstanceId(workflowId);
					theTask.setStatus(Status.READY_FOR_RERUN);
					theTask.setTaskType(previousTask.getTaskType());
					theTask.setCorrelationId(wf.getCorrelationId());
					theTask.setSeq(seq++);
					theTask.setRetryCount(previousTask.getRetryCount() + 1);
					newTasks.add(theTask);
					break;
				} else { // Create with Skipped status
					Task theTask = new Task();
					theTask.setTaskId(IDGenerator.generate());
					theTask.setReferenceTaskName(previousTask.getReferenceTaskName());
					theTask.setWorkflowInstanceId(workflowId);
					theTask.setStatus(Status.SKIPPED);
					theTask.setTaskType(previousTask.getTaskType());
					theTask.setCorrelationId(wf.getCorrelationId());
					theTask.setInputData(previousTask.getInputData());
					theTask.setOutputData(previousTask.getOutputData());
					theTask.setRetryCount(previousTask.getRetryCount() + 1);
					theTask.setSeq(seq++);
					newTasks.add(theTask);
				}
			}

			edao.createTasks(newTasks);
		}

		edao.createWorkflow(wf);

		// send wf start message
		notifyWorkflowStatus(wf, StartEndState.start);

		decide(workflowId);
		return workflowId;
	}

	public void rewind(String workflowId, Map<String, Object> headers) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (!workflow.getStatus().isTerminal()) {
			logger.error("Workflow is still running.  status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId());
			throw new ApplicationException(Code.CONFLICT, "Workflow is still running.  status=" + workflow.getStatus());
		}

		// Remove all the tasks...
		workflow.getTasks().forEach(t -> edao.removeTask(t.getTaskId()));
		workflow.getTasks().clear();
		workflow.setReasonForIncompletion(null);
		workflow.setStartTime(System.currentTimeMillis());
		workflow.setEndTime(0);
		// Change the status to running
		workflow.setStatus(WorkflowStatus.RUNNING);
		workflow.setHeaders(headers);
		edao.updateWorkflow(workflow);

		// send wf start message
		notifyWorkflowStatus(workflow, StartEndState.start);

		decide(workflowId);
	}

	public void retry(String workflowId, Map<String, Object> headers) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (!workflow.getStatus().isTerminal()) {
			logger.error("Workflow is still running.  status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId());
			throw new ApplicationException(Code.CONFLICT, "Workflow is still running.  status=" + workflow.getStatus());
		}
		if (workflow.getTasks().isEmpty()) {
			logger.error("Workflow has not started yet.  status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId());
			throw new ApplicationException(Code.CONFLICT, "Workflow has not started yet");
		}
		int lastIndex = workflow.getTasks().size() - 1;
		Task last = workflow.getTasks().get(lastIndex);
		if (!last.getStatus().isTerminal()) {
			throw new ApplicationException(Code.CONFLICT,
					"The last task is still not completed!  I can only retry the last failed task.  Use restart if you want to attempt entire workflow execution again.");
		}
		if (last.getStatus().isSuccessful()) {
			throw new ApplicationException(Code.CONFLICT,
					"The last task has not failed!  I can only retry the last failed task.  Use restart if you want to attempt entire workflow execution again.");
		}

		// Below is the situation where currently when the task failure causes
		// workflow to fail, the task's retried flag is not updated. This is to
		// update for these old tasks.
		List<Task> update = workflow.getTasks().stream().filter(task -> !task.isRetried()).collect(Collectors.toList());
		update.forEach(task -> task.setRetried(true));
		edao.updateTasks(update);

		Task retried = last.copy();
		retried.setTaskId(IDGenerator.generate());
		retried.setRetriedTaskId(last.getTaskId());
		retried.setStatus(Status.SCHEDULED);
		retried.setRetryCount(last.getRetryCount() + 1);
		scheduleTask(workflow, Arrays.asList(retried));

		workflow.setStatus(WorkflowStatus.RUNNING);
		workflow.setHeaders(headers);
		edao.updateWorkflow(workflow);

		decide(workflowId);
	}

	public List<Workflow> getStatusByCorrelationId(String workflowName, String correlationId, boolean includeClosed) throws Exception {
		Preconditions.checkNotNull(correlationId, "correlation id is missing");
		Preconditions.checkNotNull(workflowName, "workflow name is missing");
		List<Workflow> workflows = edao.getWorkflowsByCorrelationId(correlationId);
		List<Workflow> result = new LinkedList<>();
		for (Workflow wf : workflows) {
			if (wf.getWorkflowType().equals(workflowName) && (includeClosed || wf.getStatus().equals(WorkflowStatus.RUNNING))) {
				result.add(wf);
			}
		}

		return result;
	}

	public Task getPendingTaskByWorkflow(String taskReferenceName, String workflowId) {
		List<Task> tasks = edao.getTasksForWorkflow(workflowId).stream()
				.filter(task -> !task.getStatus().isTerminal() && task.getReferenceTaskName().equals(taskReferenceName)).collect(Collectors.toList());
		if (!tasks.isEmpty()) {
			return tasks.get(0); // There can only be one task by a given
			// reference name running at a time.
		}
		return null;
	}

	public void completeWorkflow(Workflow wf) throws Exception {
		Workflow workflow = edao.getWorkflow(wf.getWorkflowId(), false);

		if (workflow.getStatus().equals(WorkflowStatus.COMPLETED)) {
			logger.warn("Workflow has already been completed.  Current status=" + workflow.getStatus() + ", workflowId=" + wf.getWorkflowId()+",CorrelationId=" + wf.getCorrelationId());
			return;
		}

		if (workflow.getStatus().isTerminal()) {
			String msg = "Workflow has already been completed.  Current status " + workflow.getStatus();
			logger.error("Workflow has already been completed.  status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId());
			throw new ApplicationException(Code.CONFLICT, msg);
		}

		workflow.setStatus(WorkflowStatus.COMPLETED);
		workflow.setOutput(wf.getOutput());
		edao.updateWorkflow(workflow);

		// If the following task, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			Workflow parent = edao.getWorkflow(workflow.getParentWorkflowId(), false);
			decide(parent.getWorkflowId());
		}
		Monitors.recordWorkflowCompletion(workflow.getWorkflowType(), workflow.getEndTime() - workflow.getStartTime());
		queue.remove(deciderQueue, workflow.getWorkflowId());	//remove from the sweep queue

		// send wf end message
		notifyWorkflowStatus(wf, StartEndState.end);

		logger.info("Workflow has completed, workflowId=" + wf.getWorkflowId()+",input="+wf.getInput()+",CorrelationId="+wf.getCorrelationId()+",output="+wf.getOutput());
	}

	public String cancelWorkflow(String workflowId,Map<String, Object> inputbody) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		workflow.setStatus(WorkflowStatus.CANCELLED);
		return cancelWorkflow(workflow,inputbody);
	}

	public String cancelWorkflow(Workflow workflow, Map<String, Object> inputbody) throws Exception {

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.CANCELLED);
		}

		String workflowId = workflow.getWorkflowId();
		edao.updateWorkflow(workflow);
		logger.error("Workflow is cancelled.workflowId="+workflowId+",correlationId="+workflow.getCorrelationId());
		List<Task> tasks = workflow.getTasks();
		for (Task task : tasks) {
			if (!task.getStatus().isTerminal()) {
				// Cancel the ones which are not completed yet....
				task.setStatus(Status.CANCELED);
				if (SystemTaskType.is(task.getTaskType())) {
					WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
					stt.cancel(workflow, task, this);
					//SystemTaskType.valueOf(task.getTaskType()).cancel(workflow, task, this);
				}
				edao.updateTask(task);
				notifyTaskStatus(task, StartEndState.end);
			}
			// And remove from the task queue if they were there
			queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
		}

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			Workflow parent = edao.getWorkflow(workflow.getParentWorkflowId(), false);
			decide(parent.getWorkflowId());
		}

		WorkflowDef def = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
		String cancelWorkflow = def.getCancelWorkflow();
		if (!StringUtils.isBlank(cancelWorkflow)) {
			// Backward compatible by default
			boolean expandInline = Boolean.parseBoolean(config.getProperty("workflow.failure.expandInline", "true"));
			Map<String, Object> input = new HashMap<>();
			if (expandInline) {
				input.putAll(workflow.getInput());
			} else {
				input.put("workflowInput", workflow.getInput());
			}
			input.put("workflowId", workflowId);
			input.put("workflowType", workflow.getWorkflowType());
			input.put("workflowVersion", workflow.getVersion());

			try {

				WorkflowDef latestCancelWorkflow = metadata.getLatest(cancelWorkflow);
				String cancelWFId = startWorkflow(cancelWorkflow, latestCancelWorkflow.getVersion(), input, workflowId, null, null, null);
				workflow.getOutput().put("conductor.cancel_workflow", cancelWFId);

			} catch (Exception e) {
				logger.error("Error workflow " + cancelWorkflow + " failed to start.  reason: " + e.getMessage());
				workflow.getOutput().put("conductor.cancel_workflow", "Error workflow " + cancelWorkflow + " failed to start.  reason: " + e.getMessage());
				Monitors.recordWorkflowStartError(cancelWorkflow);
			}
		}

		queue.remove(deciderQueue, workflow.getWorkflowId());	//remove from the sweep queue

		// send wf end message
		notifyWorkflowStatus(workflow, StartEndState.end);

		// Send to atlas
		Monitors.recordWorkflowTermination(workflow.getWorkflowType(), workflow.getStatus());
		return workflowId;
	}

	public void terminateWorkflow(String workflowId, String reason) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		workflow.setStatus(WorkflowStatus.TERMINATED);
		terminateWorkflow(workflow, reason, null);
	}

	public void terminateWorkflow(Workflow workflow, String reason, String failureWorkflow) throws Exception {
		terminateWorkflow(workflow, reason, failureWorkflow, null);
	}

	public void terminateWorkflow(Workflow workflow, String reason, String failureWorkflow, Task failedTask) throws Exception {

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.TERMINATED);
		}

		String workflowId = workflow.getWorkflowId();
		workflow.setReasonForIncompletion(reason);
		edao.updateWorkflow(workflow);
		logger.error("Workflow is terminated.workflowId="+workflowId+",correlationId="+workflow.getCorrelationId()+",reasonForIncompletion="+reason);
		List<Task> tasks = workflow.getTasks();
		for (Task task : tasks) {
			if (!task.getStatus().isTerminal()) {
				// Cancel the ones which are not completed yet....
				task.setStatus(Status.CANCELED);
				if (SystemTaskType.is(task.getTaskType())) {
					WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
					stt.cancel(workflow, task, this);
					//SystemTaskType.valueOf(task.getTaskType()).cancel(workflow, task, this);
				}
				edao.updateTask(task);
				notifyTaskStatus(task, StartEndState.end);
			}
			// And remove from the task queue if they were there
			queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
		}

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			Workflow parent = edao.getWorkflow(workflow.getParentWorkflowId(), false);
			decide(parent.getWorkflowId());
		}

		if (!StringUtils.isBlank(failureWorkflow)) {
			// Backward compatible by default
			boolean expandInline = Boolean.parseBoolean(config.getProperty("workflow.failure.expandInline", "true"));
			Map<String, Object> input = new HashMap<>();
			if (expandInline) {
				input.putAll(workflow.getInput());
			} else {
				input.put("workflowInput", workflow.getInput());
			}
			input.put("workflowId", workflowId);
			input.put("workflowType", workflow.getWorkflowType());
			input.put("workflowVersion", workflow.getVersion());
			input.put("reason", reason);
			input.put("failureStatus", workflow.getStatus().toString());
			if (failedTask != null) {
				Map<String, Object> map = new HashMap<>();
				map.put("taskId", failedTask.getTaskId());
				map.put("input", failedTask.getInputData());
				map.put("output", failedTask.getOutputData());
				map.put("retryCount", failedTask.getRetryCount());
				map.put("referenceName", failedTask.getReferenceTaskName());
				map.put("reasonForIncompletion", failedTask.getReasonForIncompletion());
				input.put("failedTask", map);
				logger.error("Error in task execution.workflowid="+workflowId+",correlationId="+workflow.getCorrelationId()+",failedTaskid="+failedTask.getTaskId()+",taskReferenceName="+failedTask.getReferenceTaskName()+"reasonForIncompletion="+failedTask.getReasonForIncompletion());
			}

			try {

				WorkflowDef latestFailureWorkflow = metadata.getLatest(failureWorkflow);
				String failureWFId = startWorkflow(failureWorkflow, latestFailureWorkflow.getVersion(), input, workflowId, null, null, null);
				workflow.getOutput().put("conductor.failure_workflow", failureWFId);

			} catch (Exception e) {
				logger.error("Error workflow " + failureWorkflow + " failed to start.  reason: " + e.getMessage());
				workflow.getOutput().put("conductor.failure_workflow", "Error workflow " + failureWorkflow + " failed to start.  reason: " + e.getMessage());
				Monitors.recordWorkflowStartError(failureWorkflow);
			}
		}

		queue.remove(deciderQueue, workflow.getWorkflowId());	//remove from the sweep queue

		// send wf end message
		notifyWorkflowStatus(workflow, StartEndState.end);

		// Send to atlas
		Monitors.recordWorkflowTermination(workflow.getWorkflowType(), workflow.getStatus());
	}

	public void updateTask(TaskResult result) throws Exception {
		if (result == null) {
			logger.error("null task given for update..." + result);
			throw new ApplicationException(Code.INVALID_INPUT, "Task object is null");
		}
		String workflowId = result.getWorkflowInstanceId();
		Workflow wf = edao.getWorkflow(workflowId);
		Task task = edao.getTask(result.getTaskId());
		if (wf.getStatus().isTerminal()) {
			// Workflow is in terminal state
			queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
			if(!task.getStatus().isTerminal()) {
				task.setStatus(Status.COMPLETED);
			}
			task.setOutputData(result.getOutputData());
			task.setReasonForIncompletion(result.getReasonForIncompletion());
			task.setWorkerId(result.getWorkerId());
			edao.updateTask(task);
			notifyTaskStatus(task, StartEndState.end);
			String msg = "Workflow " + wf.getWorkflowId() + " is already completed as " + wf.getStatus() + ", task=" + task.getTaskType() + ", reason=" + wf.getReasonForIncompletion()+",correlationId="+wf.getCorrelationId();
			logger.warn(msg);
			Monitors.recordUpdateConflict(task.getTaskType(), wf.getWorkflowType(), wf.getStatus());
			return;
		}

		if (task.getStatus().isTerminal()) {
			// Task was already updated....
			queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
			String msg = "Task is already completed as " + task.getStatus() + "@" + task.getEndTime() + ", workflow status=" + wf.getStatus() + ", workflowId=" + wf.getWorkflowId() + ", taskId=" + task.getTaskId()+",correlationId="+wf.getCorrelationId();
			logger.warn(msg);
			Monitors.recordUpdateConflict(task.getTaskType(), wf.getWorkflowType(), task.getStatus());
			return;
		}

		task.setStatus(Status.valueOf(result.getStatus().name()));
		task.setOutputData(result.getOutputData());
		task.setReasonForIncompletion(result.getReasonForIncompletion());
		task.setWorkerId(result.getWorkerId());
		task.setCallbackAfterSeconds(result.getCallbackAfterSeconds());

		if (task.getStatus().isTerminal()) {
			task.setEndTime(System.currentTimeMillis());
		} else {
			if (result.isResetStartTime()) {
				task.setStartTime(System.currentTimeMillis());
				// We must reset endtime only when it is set
				if (task.getEndTime() > 0) {
					task.setEndTime(System.currentTimeMillis());
				}
			}
		}
		edao.updateTask(task);

		result.getLogs().forEach(tl -> tl.setTaskId(task.getTaskId()));
		edao.addTaskExecLog(result.getLogs());

		switch (task.getStatus()) {

			case COMPLETED:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				notifyTaskStatus(task, StartEndState.end);
				break;

			case CANCELED:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				notifyTaskStatus(task, StartEndState.end);
				break;
			case FAILED:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				notifyTaskStatus(task, StartEndState.end);
				break;
			case IN_PROGRESS:
				// put it back in queue based in callbackAfterSeconds
				long callBack = result.getCallbackAfterSeconds();
				queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
				queue.push(QueueUtils.getQueueName(task), task.getTaskId(), callBack); // Milliseconds
				break;
			default:
				break;
		}

		decide(workflowId);

		if (task.getStatus().isTerminal()) {
			long duration = getTaskDuration(0, task);
			long lastDuration = task.getEndTime() - task.getStartTime();
			Monitors.recordTaskExecutionTime(task.getTaskDefName(), duration, true, task.getStatus());
			Monitors.recordTaskExecutionTime(task.getTaskDefName(), lastDuration, false, task.getStatus());
		}
	}

	public List<Task> getTasks(String taskType, String startKey, int count) throws Exception {
		return edao.getTasks(taskType, startKey, count);
	}

	public List<Workflow> getRunningWorkflows(String workflowName) throws Exception {
		List<Workflow> allwf = edao.getPendingWorkflowsByType(workflowName);
		return allwf;
	}

	public List<String> getWorkflows(String name, Integer version, Long startTime, Long endTime) {
		List<Workflow> allwf = edao.getWorkflowsByType(name, startTime, endTime);
		List<String> workflows = allwf.stream().filter(wf -> wf.getVersion() == version).map(wf -> wf.getWorkflowId()).collect(Collectors.toList());
		return workflows;
	}

	public List<String> getRunningWorkflowIds(String workflowName) throws Exception {
		return edao.getRunningWorkflowIds(workflowName);
	}

	/**
	 *
	 * @param workflowId ID of the workflow to evaluate the state for
	 * @return true if the workflow has completed (success or failed), false otherwise.
	 * @throws Exception If there was an error - caller should retry in this case.
	 */
	public boolean decide(String workflowId) throws Exception {

		Workflow workflow = edao.getWorkflow(workflowId, true);
		WorkflowDef def = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
		try {
			DeciderOutcome outcome = decider.decide(workflow, def);
			if(outcome.isComplete) {
				completeWorkflow(workflow);
				return true;
			}

			List<Task> tasksToBeScheduled = outcome.tasksToBeScheduled;
			setTaskDomains(tasksToBeScheduled, workflow);
			List<Task> tasksToBeUpdated = outcome.tasksToBeUpdated;
			boolean stateChanged = false;

			workflow.getTasks().addAll(tasksToBeScheduled);
			for(Task task : tasksToBeScheduled) {
				if (SystemTaskType.is(task.getTaskType()) && !task.getStatus().isTerminal()) {
					WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
					if (!stt.isAsync() && stt.execute(workflow, task, this)) {
						tasksToBeUpdated.add(task);
						stateChanged = true;
					}
				}
			}
			stateChanged = scheduleTask(workflow, tasksToBeScheduled) || stateChanged;

			if(!outcome.tasksToBeUpdated.isEmpty() || !outcome.tasksToBeScheduled.isEmpty()) {
				edao.updateTasks(tasksToBeUpdated);
				edao.updateWorkflow(workflow);
				queue.push(deciderQueue, workflow.getWorkflowId(), config.getSweepFrequency());
			}

			if (outcome.startWorkflow != null) {
				DeciderService.StartWorkflowParams startWorkflow = outcome.startWorkflow;
				String workflowName = startWorkflow.name;
				int workflowVersion;
				if (startWorkflow.version == null) {
					WorkflowDef subFlowDef = metadata.getLatest(workflowName);
					workflowVersion = subFlowDef.getVersion();
				} else {
					workflowVersion = startWorkflow.version;
				}
				startWorkflow(workflowName, workflowVersion, startWorkflow.params, null, workflow.getWorkflowId(), null,null);
			}

			if(stateChanged) {
				decide(workflowId);
			}

		} catch (TerminateWorkflow tw) {
			logger.error("Error in workflow execution:"+tw.getMessage(), tw);
			terminate(def, workflow, tw);
			return true;
		}
		return false;
	}

	public void pauseWorkflow(String workflowId) throws Exception {
		WorkflowStatus status = WorkflowStatus.PAUSED;
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if(workflow.getStatus().isTerminal()){
			throw new ApplicationException(Code.CONFLICT, "Workflow id " + workflowId + " has ended, status cannot be updated.");
		}
		if (workflow.getStatus().equals(status)) {
			return;		//Already paused!
		}
		workflow.setStatus(status);
		edao.updateWorkflow(workflow);
	}

	public void resumeWorkflow(String workflowId) throws Exception{
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if(!workflow.getStatus().equals(WorkflowStatus.PAUSED)){
			logger.error("Workflow is not is not PAUSED so cannot resume. Current status=" + workflow.getStatus() + ", workflowId=" + workflow.getWorkflowId()+",CorrelationId=" + workflow.getCorrelationId());
			throw new IllegalStateException("The workflow " + workflowId + " is not is not PAUSED so cannot resume");
		}
		workflow.setStatus(WorkflowStatus.RUNNING);
		edao.updateWorkflow(workflow);
		decide(workflowId);
	}

	public void skipTaskFromWorkflow(String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest)  throws Exception {

		Workflow wf = edao.getWorkflow(workflowId, true);

		// If the wf is not running then cannot skip any task
		if(!wf.getStatus().equals(WorkflowStatus.RUNNING)){
			String errorMsg = String.format("The workflow %s is not running so the task referenced by %s cannot be skipped", workflowId, taskReferenceName);
			logger.error(errorMsg);
			throw new IllegalStateException(errorMsg);
		}
		// Check if the reference name is as per the workflowdef
		WorkflowDef wfd = metadata.get(wf.getWorkflowType(), wf.getVersion());
		WorkflowTask wft = wfd.getTaskByRefName(taskReferenceName);
		if(wft == null){
			String errorMsg = String.format("The task referenced by %s does not exist in the WorkflowDef %s", taskReferenceName, wf.getWorkflowType());
			logger.error(errorMsg);
			throw new IllegalStateException(errorMsg);
		}
		// If the task is already started the again it cannot be skipped
		wf.getTasks().forEach(task -> {
			if(task.getReferenceTaskName().equals(taskReferenceName)){
				String errorMsg = String.format("The task referenced %s has already been processed, cannot be skipped", taskReferenceName);
				logger.error(errorMsg);
				throw new IllegalStateException(errorMsg);
			}
		});
		// Now create a "SKIPPED" task for this workflow
		Task theTask = new Task();
		theTask.setTaskId(IDGenerator.generate());
		theTask.setReferenceTaskName(taskReferenceName);
		theTask.setWorkflowInstanceId(workflowId);
		theTask.setStatus(Status.SKIPPED);
		theTask.setTaskType(wft.getName());
		theTask.setCorrelationId(wf.getCorrelationId());
		if(skipTaskRequest != null){
			theTask.setInputData(skipTaskRequest.getTaskInput());
			theTask.setOutputData(skipTaskRequest.getTaskOutput());
		}
		edao.createTasks(Arrays.asList(theTask));
		decide(workflowId);
	}

	public Workflow getWorkflow(String workflowId, boolean includeTasks) {
		return edao.getWorkflow(workflowId, includeTasks);
	}

	public void addTaskToQueue(Task task) throws Exception {
		// put in queue
		queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
		if (task.getCallbackAfterSeconds() > 0) {
			queue.push(QueueUtils.getQueueName(task), task.getTaskId(), task.getCallbackAfterSeconds());
		} else {
			queue.push(QueueUtils.getQueueName(task), task.getTaskId(), 0);
		}
	}

	//Executes the async system task
	public void executeSystemTask(WorkflowSystemTask systemTask, String taskId, int unackTimeout) {
		try {
			Task task = edao.getTask(taskId);

			if(task.getStatus().isTerminal()) {
				//Tune the SystemTaskWorkerCoordinator's queues - if the queue size is very big this can happen!
				logger.warn("Task {}/{} was already completed.", task.getTaskType(), task.getTaskId());
				queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
				return;
			}

			String workflowId = task.getWorkflowInstanceId();
			Workflow workflow = edao.getWorkflow(workflowId, true);

			if (task.getStartTime() == 0) {
				task.setStartTime(System.currentTimeMillis());
				Monitors.recordQueueWaitTime(task.getTaskDefName(), task.getQueueWaitTime());
			}

			if(workflow.getStatus().isTerminal()) {
				logger.warn("Workflow {} has been completed for {}/{}", workflow.getWorkflowId(), systemTask.getName(), task.getTaskId());
				if(!task.getStatus().isTerminal()) {
					task.setStatus(Status.CANCELED);
				}
				edao.updateTask(task);
				queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
				notifyTaskStatus(task, StartEndState.end);
				return;
			}

			if(task.getStatus().equals(Status.SCHEDULED)) {

				if(edao.exceedsInProgressLimit(task)) {
					logger.warn("Rate limited for {}", task.getTaskDefName());
					return;
				}
			}

			logger.info("Executing {}/{}-{}", task.getTaskType(), task.getTaskId(), task.getStatus());

			queue.setUnackTimeout(QueueUtils.getQueueName(task), task.getTaskId(), systemTask.getRetryTimeInSecond() * 1000);
			task.setPollCount(task.getPollCount() + 1);
			edao.updateTask(task);

			switch (task.getStatus()) {

				case SCHEDULED:
					notifyTaskStatus(task, StartEndState.start);
					systemTask.start(workflow, task, this);
					break;

				case IN_PROGRESS:
					systemTask.execute(workflow, task, this);
					break;
				default:
					break;
			}

			if(!task.getStatus().isTerminal()) {
				task.setCallbackAfterSeconds(unackTimeout);
			}

			updateTask(new TaskResult(task));
			logger.info("Done Executing {}/{}-{} op={}", task.getTaskType(), task.getTaskId(), task.getStatus(), task.getOutputData().toString());

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void setTaskDomains(List<Task> tasks, Workflow wf){
		Map<String, String> taskToDomain = wf.getTaskToDomain();
		if(taskToDomain != null){
			// Check if all tasks have the same domain "*"
			String domainstr = taskToDomain.get("*");
			if(domainstr != null){
				String[] domains = domainstr.split(",");
				tasks.forEach(task -> {
					// Filter out SystemTask
					if(!(task instanceof SystemTask)){
						// Check which domain worker is polling
						// Set the task domain
						task.setDomain(getActiveDomain(task.getTaskType(), domains));
					}
				});

			} else {
				tasks.forEach(task -> {
					if(!(task instanceof SystemTask)){
						String taskDomainstr = taskToDomain.get(task.getTaskType());
						if(taskDomainstr != null){
							task.setDomain(getActiveDomain(task.getTaskType(), taskDomainstr.split(",")));
						}
					}
				});
			}
		}
	}

	public void updateWorkflow(Workflow workflow) {
		edao.updateWorkflow(workflow);
	}

	private String getActiveDomain(String taskType, String[] domains){
		// The domain list has to be ordered.
		// In sequence check if any worker has polled for last 30 seconds, if so that is the Active domain
		String domain = null; // Default domain
		for(String d: domains){
			PollData pd = edao.getPollData(taskType, d.trim());
			if(pd != null){
				if(pd.getLastPollTime() > System.currentTimeMillis() - (activeWorkerLastPollnSecs * 1000)){
					domain = d.trim();
					break;
				}
			}
		}
		return domain;
	}

	private long getTaskDuration(long s, Task task) {
		long duration = task.getEndTime() - task.getStartTime();
		s += duration;
		if (task.getRetriedTaskId() == null) {
			return s;
		}
		return s + getTaskDuration(s, edao.getTask(task.getRetriedTaskId()));
	}

	@VisibleForTesting
	boolean scheduleTask(Workflow workflow, List<Task> tasks) throws Exception {

		if (tasks == null || tasks.isEmpty()) {
			return false;
		}
		int count = workflow.getTasks().size();

		for (Task task : tasks) {
			task.setSeq(++count);
		}

		List<Task> created = edao.createTasks(tasks);
		List<Task> createdSystemTasks = created.stream().filter(task -> SystemTaskType.is(task.getTaskType())).collect(Collectors.toList());
		List<Task> toBeQueued = created.stream().filter(task -> !SystemTaskType.is(task.getTaskType())).collect(Collectors.toList());
		boolean startedSystemTasks = false;
		for(Task task : createdSystemTasks) {

			WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
			if(stt == null) {
				throw new RuntimeException("No system task found by name " + task.getTaskType());
			}
			task.setStartTime(System.currentTimeMillis());
			if(!stt.isAsync()) {
				notifyTaskStatus(task, StartEndState.start);
				stt.start(workflow, task, this);
				startedSystemTasks = true;
				edao.updateTask(task);
				if (task.getStatus().isTerminal()) {
					notifyTaskStatus(task, StartEndState.end);
				}
			} else {
				toBeQueued.add(task);
			}
		}
		addTaskToQueue(toBeQueued);
		return startedSystemTasks;
	}

	private void addTaskToQueue(final List<Task> tasks) throws Exception {
		for (Task t : tasks) {
			addTaskToQueue(t);
		}
	}

	private void terminate(final WorkflowDef def, final Workflow workflow, TerminateWorkflow tw) throws Exception {

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(tw.workflowStatus);
		}

		String failureWorkflow = def.getFailureWorkflow();
		if (failureWorkflow != null) {
			if (failureWorkflow.startsWith("$")) {
				String[] paramPathComponents = failureWorkflow.split("\\.");
				String name = paramPathComponents[2]; // name of the input parameter
				failureWorkflow = (String) workflow.getInput().get(name);
			}
		}
		if(tw.task != null){
			edao.updateTask(tw.task);
		}
		terminateWorkflow(workflow, tw.getMessage(), failureWorkflow, tw.task);

		String taskId = (tw.task != null ? tw.task.getTaskId() : null);
		String taskRefName = (tw.task != null ? tw.task.getReferenceTaskName() : null);
		logger.error("Workflow failed. workflowId=" + workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId()+",Reason="+tw.getMessage()+",taskId="+taskId+",taskReferenceName="+taskRefName);
	}

	@SuppressWarnings("unchecked")
	private void notifyTaskStatus(Task task, StartEndState state) {
		try {
			Map<String, Object> eventMap = task.getWorkflowTask().getEventMessages();
			if (eventMap == null || !eventMap.containsKey(state.name())) {
				return;
			}

			ParametersUtils pu = new ParametersUtils();
			Workflow workflow = edao.getWorkflow(task.getWorkflowInstanceId());
			Map<String, Object> map = pu.getTaskInputV2(eventMap, workflow, task.getTaskId(), null);
			sendMessage(map, state.name());
		} catch (Exception ex) {
			logger.error("Unable to notify task status " + state.name() + ", failed with " + ex.getMessage(), ex);
		}
	}

	@SuppressWarnings("unchecked")
	private void notifyWorkflowStatus(Workflow workflow, StartEndState state) {
		try {
			WorkflowDef workflowDef = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
			Map<String, Object> eventMap = workflowDef.getEventMessages();
			if (eventMap == null || !eventMap.containsKey(state.name())) {
				return;
			}

			ParametersUtils pu = new ParametersUtils();
			Map<String, Object> map = pu.getTaskInputV2(eventMap, workflow, null, null);
			sendMessage(map, state.name());
		} catch (Exception ex) {
			logger.error("Unable to notify workflow status " + state.name() + ", failed with " + ex.getMessage(), ex);
		}
	}

	@SuppressWarnings("unchecked")
	private void sendMessage(Map<String, Object> map, String name) throws Exception {
		Map<String, Object> actionMap = (Map<String, Object>) map.get(name);
		ObjectMapper mapper = new ObjectMapper();

		Message msg = new Message();
		msg.setId(UUID.randomUUID().toString());

		String payload = mapper.writeValueAsString(actionMap.get("inputParameters"));
		msg.setPayload(payload);

		String sink = (String) actionMap.get("sink");
		ObservableQueue queue = EventQueues.getQueue(sink, false);
		if (queue == null) {
			logger.error("sendMessage. No queue found for " + sink);
			return;
		}

		queue.publish(Collections.singletonList(msg));
	}

	private void validateWorkflowInput(WorkflowDef workflowDef, Map<String, Object> payload) {
		Map<String, String> rules = workflowDef.getInputValidation();

		rules.forEach((name, condition) -> {
			boolean success = false;
			String extra = "";
			try {
				success = ScriptEvaluator.evalBool(condition, payload);
			} catch (Exception ex) {
				extra = ": " + ex.getMessage();
			}
			if (!success) {
				String message = "Input validation failed for '" + name + "' rule" + extra;
				throw new ApplicationException(Code.INVALID_INPUT, message);
			}
		});
	}

	private void validateAuth(WorkflowDef workflowDef, Map<String, Object> headers) {
		// It gives us: Bearer token
		String bearer = (String)headers.get(HttpHeaders.AUTHORIZATION);
		if (StringUtils.isEmpty(bearer))
			throw new ApplicationException(Code.INVALID_INPUT, "No " + HttpHeaders.AUTHORIZATION + " header provided");

		// Checking bearer format
		if (!bearer.startsWith(BEARER))
			throw new ApplicationException(Code.INVALID_INPUT, "Invalid " + HttpHeaders.AUTHORIZATION + " header format");

		// Get the access token
		String token = bearer.substring(BEARER.length());

		// Do a validation
		Map<String, Object> failedList;
		try {
			failedList = auth.validate(token, workflowDef.getAuthValidation());
		} catch (Exception ex) {
			logger.error("An internal error occurred during auth validation: " + ex.getMessage(), ex);
			throw new ApplicationException(Code.INTERNAL_ERROR, "An internal error occurred during auth validation");
		}

		if (!failedList.isEmpty()) {
			throw new ApplicationException(Code.INVALID_INPUT, "Auth validation failed");
		}
	}
}

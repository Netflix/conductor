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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import java.util.*;
import java.util.function.Function;
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
		return startWorkflow(null, name, version, input, correlationId, null, null, event, taskToDomain, null);
	}

	public String startWorkflow(String workflowId, String name, int version, String correlationId, Map<String, Object> input, String event, Map<String, String> taskToDomain) throws Exception {
		return startWorkflow(workflowId, name, version, input, correlationId, null, null, event, taskToDomain, null);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event) throws Exception {
		return startWorkflow(null, name, version, input, correlationId, parentWorkflowId,  parentWorkflowTaskId, event, null, null);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, Map<String, String> taskToDomain, List<String> workflowIds) throws Exception {
		return startWorkflow(null, name, version, input, correlationId, parentWorkflowId, parentWorkflowTaskId, event, taskToDomain, workflowIds);
	}

	public String startWorkflow(String workflowId, String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, Map<String, String> taskToDomain, List<String> workflowIds) throws Exception {

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

			Set<String> missingTaskDefs = exists.all().stream()
					.filter(wft -> wft.getType().equals(WorkflowTask.Type.SIMPLE.name()))
					.map(wft2 -> wft2.getName()).filter(task -> metadata.getTaskDef(task) == null)
					.collect(Collectors.toSet());
			if(!missingTaskDefs.isEmpty()) {
				throw new ApplicationException(Code.INVALID_INPUT, "Cannot find the task definitions for the following tasks used in workflow: " + missingTaskDefs);
			}
			// If no predefined workflowId - generate one
			if (StringUtils.isEmpty(workflowId)) {
				workflowId = IDGenerator.generate();
			}

			// Persist the Workflow
			Workflow wf = new Workflow();
			wf.setWorkflowId(workflowId);
			wf.setCorrelationId(correlationId);
			wf.setWorkflowType(name);
			wf.setVersion(version);
			wf.setInput(input);
			wf.setStatus(WorkflowStatus.RUNNING);
			wf.setParentWorkflowId(parentWorkflowId);

			// Add other ids if passed
			if (CollectionUtils.isNotEmpty(workflowIds)) {
				workflowIds.forEach(id -> {
					if (!wf.getWorkflowIds().contains(id)) {
						wf.getWorkflowIds().add(id);
					}
				});
			}

			// Add parent workflow id
			if (StringUtils.isNotEmpty(parentWorkflowId)) {
				if (!wf.getWorkflowIds().contains(parentWorkflowId)) {
					wf.getWorkflowIds().add(parentWorkflowId);
				}
			}

			// Add current id
			if (!wf.getWorkflowIds().contains(workflowId)) {
				wf.getWorkflowIds().add(workflowId);
			}

			wf.setParentWorkflowTaskId(parentWorkflowTaskId);
			wf.setOwnerApp(WorkflowContext.get().getClientApp());
			wf.setCreateTime(System.currentTimeMillis());
			wf.setUpdatedBy(null);
			wf.setUpdateTime(null);
			wf.setEvent(event);
			wf.setTaskToDomain(taskToDomain);
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
		Preconditions.checkNotNull(request.getReRunFromWorkflowId(), "reRunFromWorkflowId is missing");
		if(!rerunWF(request.getReRunFromWorkflowId(), request.getReRunFromTaskId(), request.getTaskInput(),
				request.getWorkflowInput(), request.getCorrelationId())){
			throw new ApplicationException(Code.INVALID_INPUT, "Task " + request.getReRunFromTaskId() + " not found");
		}
		return request.getReRunFromWorkflowId();
	}

	private boolean rerunWF(String workflowId, String taskId, Map<String, Object> taskInput,
							Map<String, Object> workflowInput, String correlationId) throws Exception{

		// Get the workflow
		Workflow workflow = edao.getWorkflow(workflowId);

		// If the task Id is null it implies that the entire workflow has to be rerun
		if(taskId == null){
			// remove all tasks
			workflow.getTasks().forEach(t -> edao.removeTask(t.getTaskId()));
			// Set workflow as RUNNING
			workflow.setStatus(WorkflowStatus.RUNNING);
			if(StringUtils.isNotEmpty(correlationId)){
				workflow.setCorrelationId(correlationId);
			}
			if(workflowInput != null){
				workflow.setInput(workflowInput);
			}

			edao.updateWorkflow(workflow);

			// send wf start message
			notifyWorkflowStatus(workflow, StartEndState.start);

			decide(workflowId);
			return true;
		}

		// Now iterate thru the tasks and find the "specific" task
		Task theTask = null;
		for(Task t: workflow.getTasks()){
			if(t.getTaskId().equals(taskId)){
				theTask = t;
				break;
			} else {
				// If not found look into sub workflows
				if(t.getTaskType().equalsIgnoreCase("SUB_WORKFLOW")){
					String subWorkflowId = t.getInputData().get("subWorkflowId").toString();
					if(rerunWF(subWorkflowId, taskId, taskInput, null, null)){
						theTask = t;
						break;
					}
				}
			}
		}

		if(theTask != null){
			// Remove all later tasks from the "theTask"
			for(Task t: workflow.getTasks()){
				if(t.getSeq() > theTask.getSeq()){
					edao.removeTask(t.getTaskId());
				}
			}
			if(theTask.getTaskType().equalsIgnoreCase("SUB_WORKFLOW")){
				// if task is sub workflow set task as IN_PROGRESS
				theTask.setStatus(Status.IN_PROGRESS);
				edao.updateTask(theTask);
			} else {
				// Set the task to rerun
				theTask.setStartTime(0);
				theTask.setEndTime(0);
				theTask.setStatus(Status.SCHEDULED);
				if(taskInput != null){
					theTask.setInputData(taskInput);
				}
				theTask.setRetried(false);
				edao.updateTask(theTask);
				addTaskToQueue(theTask);
			}
			// and workflow as RUNNING
			workflow.setStatus(WorkflowStatus.RUNNING);
			if(StringUtils.isNotEmpty(correlationId)){
				workflow.setCorrelationId(correlationId);
			}
			if(workflowInput != null){
				workflow.setInput(workflowInput);
			}

			edao.updateWorkflow(workflow);

			// send wf start message
			notifyWorkflowStatus(workflow, StartEndState.start);

			decide(workflowId);
			return true;
		}
		logger.info("Workflow rerun.Current status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()+",CorrelationId=" + workflow.getCorrelationId()+",input="+workflow.getInput());
		return false;
	}

	public void rewind(String workflowId, String correlationId) throws Exception {
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
		if(StringUtils.isNotEmpty(correlationId)) {
			workflow.setCorrelationId(correlationId);
		}
		edao.updateWorkflow(workflow);

		// send wf start message
		notifyWorkflowStatus(workflow, StartEndState.start);

		decide(workflowId);
		logger.info("Workflow rewind.Current status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()+",CorrelationId=" + workflow.getCorrelationId()+",input="+workflow.getInput());
	}

	public void retry(String workflowId, String correlationId) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (!workflow.getStatus().isTerminal()) {
			logger.error("Workflow is still running.  status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId());
			throw new ApplicationException(Code.CONFLICT, "Workflow is still running.  status=" + workflow.getStatus());
		}
		if (workflow.getTasks().isEmpty()) {
			logger.error("Workflow has not started yet.  status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId());
			throw new ApplicationException(Code.CONFLICT, "Workflow has not started yet");
		}

		// First get the failed tasks and the cancelled task
		Map<String, Task> failedMap = new HashMap<>();
		Map<String, Task> cancelledMap = new HashMap<>();
		for (Task t : workflow.getTasks()) {
			if (t.getStatus().equals(Status.FAILED)) {
				// The failed join cannot be retried (that causes immediate workflow termination),
				// hence we move it to cancelled list, eventually it gets IN_PROGRESS status back
				if (t.getTaskType().equalsIgnoreCase(WorkflowTask.Type.JOIN.toString())) {
					cancelledMap.put(t.getReferenceTaskName(), t);
				} else {
					failedMap.put(t.getReferenceTaskName(), t);
				}
			} else if (t.getStatus().equals(Status.CANCELED)) {
				cancelledMap.put(t.getReferenceTaskName(), t);
			}
		}
		List<Task> failedTasks  = new ArrayList<Task>(failedMap.values());
		List<Task> cancelledTasks = new ArrayList<Task>(cancelledMap.values());

		WorkflowDef workflowDef = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
		List<String> forbiddenTypes = workflowDef.getRetryForbidden();

		// Additional checking
		for (Task failedTask : failedTasks) {
			if (!failedTask.getStatus().isTerminal()) {
				throw new ApplicationException(Code.CONFLICT,
						"The last task is still not completed!  I can only retry the last failed task.  Use restart if you want to attempt entire workflow execution again.");
			}
			if (failedTask.getStatus().isSuccessful()) {
				throw new ApplicationException(Code.CONFLICT,
						"The last task has not failed!  I can only retry the last failed task.  Use restart if you want to attempt entire workflow execution again.");
			}

			if (forbiddenTypes.contains(failedTask.getTaskType())) {
				String message = String.format("The last task is %s! Retry is not allowed for such type of the task %s",
						failedTask.getReferenceTaskName(), failedTask.getTaskType());
				throw new ApplicationException(Code.CONFLICT, "Unable to retry from this point.  Please restart.");
			}
		}

		// Below is the situation where currently when the task failure causes
		// workflow to fail, the task's retried flag is not updated. This is to
		// update for these old tasks.
		List<Task> update = workflow.getTasks().stream().filter(task -> !task.isRetried()).collect(Collectors.toList());
		update.forEach(task -> task.setRetried(true));
		edao.updateTasks(update);

		// Just helper function to avoid duplicate code for failed/cancelled lists
		final Function<Task, Task> cloneFunc = input -> {
			Task retried = input.copy();
			retried.setTaskId(IDGenerator.generate());
			retried.setRetriedTaskId(input.getTaskId());
			retried.setStatus(Status.SCHEDULED);
			retried.setRetryCount(input.getRetryCount() + 1);
			retried.setOutputData(new HashMap<>());
			retried.setPollCount(0);
			retried.setWorkerId(null);
			retried.setStartDelayInSeconds(0);
			retried.setCallbackAfterSeconds(0);
			return retried;
		};

		// Now reschedule the failed task
		List<Task> rescheduledTasks = new ArrayList<>();
		for (Task failedTask : failedTasks) {
			Task retried = cloneFunc.apply(failedTask);
			rescheduledTasks.add(retried);
		}

		// Reschedule the cancelled task but if the join is cancelled set that to in progress
		cancelledTasks.forEach(t -> {
			if(t.getTaskType().equalsIgnoreCase(WorkflowTask.Type.JOIN.toString())){
				t.setStatus(Status.IN_PROGRESS);
				t.setRetried(false);
				edao.updateTask(t);
			} else {
				Task copy = cloneFunc.apply(t);
				rescheduledTasks.add(copy);
			}
		});

		scheduleTask(workflow, rescheduledTasks);

		workflow.setStatus(WorkflowStatus.RUNNING);
		if(StringUtils.isNotEmpty(correlationId)) {
			workflow.setCorrelationId(correlationId);
		}
		edao.updateWorkflow(workflow);

		decide(workflowId);
		logger.info("Workflow retry.Current status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()+",CorrelationId=" + workflow.getCorrelationId()+",input="+workflow.getInput());
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
		notifyWorkflowStatus(workflow, StartEndState.end);

		logger.info("Workflow has completed, workflowId=" + wf.getWorkflowId()+",input="+wf.getInput()+",CorrelationId="+wf.getCorrelationId()+",output="+wf.getOutput());
	}

	public String cancelWorkflow(String workflowId) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		workflow.setStatus(WorkflowStatus.CANCELLED);
		return cancelWorkflow(workflow, null);
	}

	public String cancelWorkflow(Workflow workflow, String reason) throws Exception {

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.CANCELLED);
		}

		String workflowId = workflow.getWorkflowId();
		workflow.setReasonForIncompletion(reason);
		edao.updateWorkflow(workflow);
		logger.error("Workflow is cancelled.workflowId="+workflowId+",correlationId="+workflow.getCorrelationId());
		cancelTasks(workflow, workflow.getTasks());

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
				String cancelWFId = startWorkflow(cancelWorkflow, latestCancelWorkflow.getVersion(), input, workflow.getCorrelationId(), workflow.getWorkflowId(), null, null);

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
		logger.info("Workflow has cancelled, workflowId=" + workflow.getWorkflowId()+",input="+workflow.getInput()+",CorrelationId="+workflow.getCorrelationId()+",output="+workflow.getOutput());
		return workflowId;
	}

	public String reset(String workflowId, String reason) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.RESET);
		}

		workflow.setReasonForIncompletion(reason);
		edao.updateWorkflow(workflow);
		logger.error("Workflow has been reset.workflowId="+workflowId+",correlationId="+workflow.getCorrelationId());
		cancelTasks(workflow, workflow.getTasks());

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			Workflow parent = edao.getWorkflow(workflow.getParentWorkflowId(), false);
			decide(parent.getWorkflowId());
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
		logger.error("Workflow is terminated/reset.workflowId="+workflowId+",correlationId="+workflow.getCorrelationId()+",reasonForIncompletion="+reason);
		List<Task> tasks = workflow.getTasks();
		cancelTasks(workflow, tasks);

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
				String failureWFId=startWorkflow(failureWorkflow, latestFailureWorkflow.getVersion(), input, workflow.getCorrelationId(), workflow.getWorkflowId(), null, null);
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
		if (MapUtils.isNotEmpty(result.getInputData())) {
			task.setInputData(result.getInputData());
		}

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
			case RESET:
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

	public void pauseWorkflow(String workflowId,String correlationId) throws Exception {
		WorkflowStatus status = WorkflowStatus.PAUSED;
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if(workflow.getStatus().isTerminal()){
			throw new ApplicationException(Code.CONFLICT, "Workflow id " + workflowId + " has ended, status cannot be updated.");
		}
		if (workflow.getStatus().equals(status)) {
			return;		//Already paused!
		}
		workflow.setStatus(status);
		if(StringUtils.isNotEmpty(correlationId)) {
			workflow.setCorrelationId(correlationId);
		}
		edao.updateWorkflow(workflow);
	}

	public void resumeWorkflow(String workflowId,String correlationId) throws Exception{
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if(!workflow.getStatus().equals(WorkflowStatus.PAUSED)){
			logger.error("Workflow is not is not PAUSED so cannot resume. Current status=" + workflow.getStatus() + ", workflowId=" + workflow.getWorkflowId()+",CorrelationId=" + workflow.getCorrelationId());
			throw new IllegalStateException("The workflow " + workflowId + " is not is not PAUSED so cannot resume");
		}
		workflow.setStatus(WorkflowStatus.RUNNING);
		if(StringUtils.isNotEmpty(correlationId)) {
			workflow.setCorrelationId(correlationId);
		}
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
		logger.error("Workflow failed/reset. workflowId=" + workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId()+",Reason="+tw.getMessage()+",taskId="+taskId+",taskReferenceName="+taskRefName);
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

	public void validateAuth(String workflowId, HttpHeaders headers) {
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if (workflow == null) {
			throw new ApplicationException(Code.NOT_FOUND, "No such workflow found for workflowId=" + workflowId);
		}

		WorkflowDef workflowDef = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
		if (workflowDef == null) {
			throw new ApplicationException(Code.NOT_FOUND, "No such workflow definition found by name=" + workflow.getWorkflowType() + ", version=" + workflow.getVersion());
		}

		validateAuth(workflowDef, headers);
	}

	public void validateAuth(WorkflowDef workflowDef, HttpHeaders headers) {
		if (!this.validateAuth || MapUtils.isEmpty(workflowDef.getAuthValidation())) {
			return;
		}

		List<String> strings = headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
		if (strings == null || strings.isEmpty())
			throw new ApplicationException(Code.UNAUTHORIZED, "No " + HttpHeaders.AUTHORIZATION + " header provided");

		// It gives us: Bearer token
		String bearer = strings.get(0);
		if (StringUtils.isEmpty(bearer))
			throw new ApplicationException(Code.UNAUTHORIZED, "No " + HttpHeaders.AUTHORIZATION + " header provided");

		// Checking bearer format
		if (!bearer.startsWith(BEARER))
			throw new ApplicationException(Code.UNAUTHORIZED, "Invalid " + HttpHeaders.AUTHORIZATION + " header format");

		// Get the access token
		String token = bearer.substring(BEARER.length());

		// Do a validation
		Map<String, Object> failedList;
		try {
			failedList = auth.validate(token, workflowDef.getAuthValidation());
		} catch (Exception ex) {
			logger.error("Auth validation failed with " + ex.getMessage(), ex);
			throw new ApplicationException(Code.UNAUTHORIZED, ex.getMessage());
		}

		if (!failedList.isEmpty()) {
			throw new ApplicationException(Code.UNAUTHORIZED, "Auth validation failed: at least one of the verify conditions failed");
		}
	}

	private void cancelTasks(Workflow workflow, List<Task> tasks) throws Exception {
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
	}
}

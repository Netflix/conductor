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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.common.metadata.events.EventPublished;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.common.run.WorkflowErrorRegistry;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.DeciderService.DeciderOutcome;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.common.run.ErrorLookup;
import com.netflix.conductor.dao.ErrorLookupDAO;
import com.netflix.conductor.service.MetricService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.text.MessageFormat;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author Viren Workflow services provider interface
 */
@Trace
public class WorkflowExecutor {
	private static final int MAX_REJECT_SIZE = 5 * 1024 * 1024;
	private static final int MAX_ALERT_SIZE = 600 * 1024;
	private static final String BEARER = "Bearer";
	private static Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);

	private MetadataDAO metadata;

	private ExecutionDAO edao;

	private QueueDAO queue;

	private ErrorLookupDAO errorLookupDAO;

	private DeciderService decider;

	private ObjectMapper om;
	private Configuration config;

	private AuthManager auth;

	public static final String deciderQueue = "_deciderQueue";
	public static final String sweeperQueue = "_sweeperQueue";

	private int activeWorkerLastPollnSecs;
	private TaskStatusListener taskStatusListener;
	private WorkflowStatusListener workflowStatusListener;

	private boolean validateAuth;
	private boolean traceIdEnabled;
	private boolean authContextEnabled;
	private boolean lazyDecider;

	private ParametersUtils pu = new ParametersUtils();

	@Inject
	public WorkflowExecutor(MetadataDAO metadata, ExecutionDAO edao, QueueDAO queue, ErrorLookupDAO errorLookupDAO,ObjectMapper om,
							AuthManager auth, Configuration config,
							TaskStatusListener taskStatusListener,
							WorkflowStatusListener workflowStatusListener) {
		this.metadata = metadata;
		this.edao = edao;
		this.queue = queue;
		this.errorLookupDAO = errorLookupDAO;
		this.om = om;
		this.config = config;
		this.auth = auth;
		this.activeWorkerLastPollnSecs = config.getIntProperty("tasks.active.worker.lastpoll", 10);
		this.taskStatusListener = taskStatusListener;
		this.workflowStatusListener = workflowStatusListener;
		this.decider = new DeciderService(metadata, om, config);
		this.validateAuth = Boolean.parseBoolean(config.getProperty("workflow.auth.validate", "false"));
		this.traceIdEnabled = Boolean.parseBoolean(config.getProperty("workflow.traceid.enabled", "false"));
		this.authContextEnabled = Boolean.parseBoolean(config.getProperty("workflow.authcontext.enabled", "false"));
		this.lazyDecider = Boolean.parseBoolean(config.getProperty("workflow.lazy.decider", "false"));
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input) throws Exception {
		return startWorkflow(name, version, correlationId, input, null);
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input, String event) throws Exception {
		return startWorkflow(name, version, input, correlationId, null, null, event);
	}

	public String startWorkflow(String name, int version, String correlationId, Map<String, Object> input, String event, Map<String, String> taskToDomain) throws Exception {
		return startWorkflow(null, name, version, input, correlationId, null, null, event, taskToDomain, null, null, null, null, null, false);
	}

	public String startWorkflow(String workflowId, String name, int version, String correlationId, Map<String, Object> input, String event, Map<String, String> taskToDomain, Map<String, Object> authorization, String contextToken, String contextUser, String traceId, boolean asyncStart) throws Exception {
		return startWorkflow(workflowId, name, version, input, correlationId, null, null, event, taskToDomain, null, authorization, contextToken, contextUser, traceId, asyncStart);
	}
	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event) throws Exception {
		return startWorkflow(null, name, version, input, correlationId, parentWorkflowId,  parentWorkflowTaskId, event, null, null, null, null, null, null, false);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, Map<String, String> taskToDomain, List<String> workflowIds, String traceId) throws Exception {
		return startWorkflow(null, name, version, input, correlationId, parentWorkflowId, parentWorkflowTaskId, event, taskToDomain, workflowIds, null, null, null, traceId, false);
	}

	public String startWorkflow(String name, int version, Map<String, Object> input, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, Map<String, String> taskToDomain, List<String> workflowIds, Map<String, Object> authorization, String contextToken, String contextUser, String traceId, boolean deciderInSweeper) throws Exception {
		return startWorkflow(null, name, version, input, correlationId, parentWorkflowId, parentWorkflowTaskId, event, taskToDomain, workflowIds, authorization, contextToken, contextUser, traceId, deciderInSweeper);
	}

	public String startWorkflow(String workflowId, String name, int version, Map<String, Object> input,
								String correlationId, String parentWorkflowId, String parentWorkflowTaskId,
								String event, Map<String, String> taskToDomain, List<String> workflowIds,
								Map<String, Object> authorization, String contextToken, String contextUser,
								String traceId, boolean deciderInSweeper) throws Exception {
		// If no predefined workflowId - generate one
		if (StringUtils.isEmpty(workflowId)) {
			workflowId = IDGenerator.generate();
		} else {
			// Make sure there is no workflow with given id
			Workflow wf = edao.getWorkflow(workflowId, false);
			if (wf != null) {
				throw new ApplicationException(Code.NOT_FOUND, "Workflow already exists for given id " + workflowId);
			}
		}

		try {
			if(input == null){
				throw new ApplicationException(Code.INVALID_INPUT, "NULL input passed when starting workflow");
			}

			WorkflowDef exists = metadata.get(name, version);
			if (exists == null) {
				throw new ApplicationException(Code.NOT_FOUND, "No such workflow defined. name=" + name + ", version=" + version);
			}
			String clientId = null;
			if(authorization != null && !authorization.isEmpty()) {
				clientId = (String)authorization.get("clientId");
			}

			// Check payload size
			String payload = om.writeValueAsString(input);
			if (payload.length() >= MAX_REJECT_SIZE) {
				throw new ApplicationException(Code.INVALID_INPUT, "Input is too long");
			} else if (payload.length() >= MAX_ALERT_SIZE) {
				logger.warn("Workflow payload size exceeds threshold. WorkflowId=" + workflowId
					+ ",correlationId=" + correlationId + ",contextUser=" + contextUser
					+ ",clientId=" + clientId + ",traceId=" + traceId);
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

			// Persist the Workflow
			Workflow wf = new Workflow();
			wf.setWorkflowId(workflowId);
			wf.setCorrelationId(correlationId);
			wf.setWorkflowType(name);
			wf.setVersion(version);
			wf.setInput(input);
			wf.setStatus(WorkflowStatus.RUNNING);
			wf.setParentWorkflowId(parentWorkflowId);
			wf.setContextToken(contextToken);
			wf.setAuthorization(authorization);
			wf.setClientId(clientId);
			wf.setContextUser(contextUser);
			if (traceIdEnabled) {
				wf.setTraceId(traceId);
			}
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
			wf.setAttributes(exists.getAttributes());

			if (StringUtils.isNotEmpty(exists.getTags())) {
				Map<String, Map<String, Object>> inputMap = pu.getInputMap(null, wf, null, null);
				List<Object> candidates = ScriptEvaluator.evalJqAsList(exists.getTags(), inputMap);
				Set<String> tags = candidates.stream().filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toSet());
				wf.setTags(tags);
			}
			edao.createWorkflow(wf);

			// metrics
			MetricService.getInstance().workflowStart(name);

			// send wf start message
			workflowStatusListener.onWorkflowStarted(wf);

			// Who calls decider ? Sweeper or current thread?
			if (deciderInSweeper) {
				wakeUpSweeper(workflowId);
			} else {
				decide(workflowId);
			}
			logger.debug("Workflow has started. Current status=" + wf.getStatus() + ",workflowId=" + wf.getWorkflowId()
				+ ",correlationId=" + wf.getCorrelationId() + ",contextUser=" + wf.getContextUser()
				+ ",clientId=" + wf.getClientId() + ",traceId=" + wf.getTraceId());
			return workflowId;

		}catch (Exception e) {
			removeQuietly(workflowId);
			MetricService.getInstance().workflowStartFailed(name);
			throw e;
		}
	}

	public String rerun(RerunWorkflowRequest request) throws Exception {
		Preconditions.checkNotNull(request.getReRunFromWorkflowId(), "reRunFromWorkflowId is missing");
		if(!rerunWF(request.getReRunFromWorkflowId(), request.getReRunFromTaskId(), request.getTaskInput(),
				request.getWorkflowInput(), request.getCorrelationId(), request.getResumeParents(), request.getTaskOutput())){
			throw new ApplicationException(Code.INVALID_INPUT, "Task " + request.getReRunFromTaskId() + " not found");
		}
		return request.getReRunFromWorkflowId();
	}

	private boolean rerunWF(String workflowId, String taskId, Map<String, Object> taskInput,
							Map<String, Object> workflowInput, String correlationId,
							Boolean resumeParents, Map<String, Object> taskOutput) throws Exception{

		// Get the workflow
		Workflow workflow = edao.getWorkflow(workflowId);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		// If the task Id is null it implies that the entire workflow has to be rerun
		if( taskId == null) {
			// remove all tasks
			workflow.getTasks().forEach(t -> edao.removeTask(t.getTaskId()));
			// Set workflow as RUNNING
			workflow.incRerunCount();
			workflow.setStatus(WorkflowStatus.RUNNING);
			workflow.setReasonForIncompletion(null);
			if(StringUtils.isNotEmpty(correlationId)){
				workflow.setCorrelationId(correlationId);
			}
			if(workflowInput != null){
				workflow.setInput(workflowInput);
			}

			workflow.setResetTags(false);
			edao.updateWorkflow(workflow);

			// metrics
			MetricService.getInstance().workflowRerun(workflow.getWorkflowType());

			// send wf start message
			workflowStatusListener.onWorkflowStarted(workflow);

			decide(workflowId);
			return true;
		}

		// Now iterate thru the tasks and find the "specific" task
		Task theTask = null;
		for (Task t: workflow.getTasks()) {
			if (t.getTaskId().equals(taskId)) {
				theTask = t;
				break;
			} else {
				// If not found look into sub workflows
				if (t.getTaskType().equalsIgnoreCase("SUB_WORKFLOW")) {
					String subWorkflowId = t.getInputData().get("subWorkflowId").toString();
					if(rerunWF(subWorkflowId, taskId, taskInput, null, null, false, null)){
						theTask = t;
						break;
					}
				}
			}
		}

		if (theTask != null) {
			// Remove all later tasks from the "theTask"
			for (Task t : workflow.getTasks()) {
				if (t.getSeq() > theTask.getSeq()) {
					edao.removeTask(t.getTaskId());
				}
			}

			// and workflow as RUNNING
			workflow.incRerunCount();
			workflow.setStatus(WorkflowStatus.RUNNING);
			workflow.setReasonForIncompletion(null);
			if (StringUtils.isNotEmpty(correlationId)) {
				workflow.setCorrelationId(correlationId);
			}
			if (workflowInput != null) {
				workflow.setInput(workflowInput);
			}

			workflow.setResetTags(false);
			edao.updateWorkflow(workflow);

			// metrics
			MetricService.getInstance().workflowRerun(workflow.getWorkflowType());

			// send wf start message
			workflowStatusListener.onWorkflowStarted(workflow);

			// resume parent workflow and the sub_workflow task (if any at all)
			if (BooleanUtils.isTrue(resumeParents)) {
				resumeParent(workflow.getParentWorkflowId(), workflowId);
			}

			// If task output provided - consider it as completed
			if (taskOutput != null) {
				theTask.setStartTime(System.currentTimeMillis());
				theTask.setEndTime(System.currentTimeMillis());
				theTask.setScheduledTime(System.currentTimeMillis());
				theTask.setStatus(Status.COMPLETED);
				if (taskInput != null) {
					theTask.setInputData(taskInput);
				}
				theTask.setOutputData(taskOutput);
				theTask.setRetried(false);
				theTask.setReasonForIncompletion(null);
				edao.updateTask(theTask);

				taskStatusListener.onTaskFinished(theTask);
			} else {
				// Start/schedule the task
				if (theTask.getTaskType().equalsIgnoreCase("SUB_WORKFLOW")) {
					// if task is sub workflow set task as IN_PROGRESS
					theTask.setStatus(Status.IN_PROGRESS);
					theTask.setStartTime(System.currentTimeMillis());
					theTask.setEndTime(0);
					theTask.setReasonForIncompletion(null);
					edao.updateTask(theTask);
				} else {
					WorkflowSystemTask stt = WorkflowSystemTask.get(theTask.getTaskType());
					if (stt.isAsync()) {
						// Set the task to rerun
						theTask.setStartTime(0);
						theTask.setEndTime(0);
						theTask.setScheduledTime(System.currentTimeMillis());
						theTask.setStatus(Status.SCHEDULED);
						if (taskInput != null) {
							theTask.setInputData(taskInput);
						}
						theTask.setRetried(false);
						theTask.setReasonForIncompletion(null);
						edao.updateTask(theTask);

						addTaskToQueue(theTask);
					} else {
						theTask.setScheduledTime(System.currentTimeMillis());
						theTask.setStartTime(System.currentTimeMillis());
						theTask.setEndTime(0);
						theTask.setStatus(Status.SCHEDULED);
						theTask.setRetried(false);
						theTask.setReasonForIncompletion(null);
						edao.updateTask(theTask);

						taskStatusListener.onTaskStarted(theTask);
						stt.start(workflow, theTask, this);

						edao.updateTask(theTask);
						if (theTask.getStatus().isTerminal()) {
							taskStatusListener.onTaskFinished(theTask);
						}
					}
				}
			}

			decide(workflowId);
			return true;
		}
		logger.debug("Workflow rerun. Current status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()
			+ ",correlationId=" + workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId()
			+ ",contextUser=" + workflow.getContextUser() + ",clientId=" + workflow.getClientId());
		return false;
	}

	public void rewind(String workflowId, String correlationId) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		if (!workflow.getStatus().isTerminal()) {
			logger.debug("Workflow is still running. status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId()+ ",contextUser=" + workflow.getContextUser()+",clientId=" + workflow.getClientId());
			throw new ApplicationException(Code.CONFLICT, "Workflow is still running. status=" + workflow.getStatus());
		}

		// Remove all the tasks...
		workflow.getTasks().forEach(t -> edao.removeTask(t.getTaskId()));
		workflow.getTasks().clear();
		workflow.setReasonForIncompletion(null);
		workflow.setStartTime(System.currentTimeMillis());
		workflow.setEndTime(0);
		workflow.incRestartCount();
		// Change the status to running
		workflow.setStatus(WorkflowStatus.RUNNING);
		if(StringUtils.isNotEmpty(correlationId)) {
			workflow.setCorrelationId(correlationId);
		}
		workflow.setResetTags(false);
		edao.updateWorkflow(workflow);

		// metrics
		MetricService.getInstance().workflowRestart(workflow.getWorkflowType());

		// send wf start message
		workflowStatusListener.onWorkflowStarted(workflow);

		decide(workflowId);
		logger.debug("Workflow rewind. Current status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()
			+ ",correlationId=" + workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId()
			+ ",contextUser=" + workflow.getContextUser() + ",clientId=" + workflow.getClientId());
	}

	public void retry(String workflowId, String correlationId) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		if (!workflow.getStatus().isTerminal()) {
			logger.debug("Workflow is still running. status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()
				+ ",correlationId="+workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId()
				+ ",contextUser=" + workflow.getContextUser()+",clientId=" + workflow.getClientId());
			throw new ApplicationException(Code.CONFLICT, "Workflow is still running.  status=" + workflow.getStatus());
		}
		if (workflow.getTasks().isEmpty()) {
			logger.debug("Workflow has not started yet. status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()
				+ ",correlationId="+workflow.getCorrelationId()+ ",traceId=" + workflow.getTraceId()
				+ ",contextUser=" + workflow.getContextUser()+",clientId=" + workflow.getClientId());
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
		workflow.setResetTags(false);
		edao.updateWorkflow(workflow);

		// metrics
		MetricService.getInstance().workflowRetry(workflow.getWorkflowType());

		decide(workflowId);
		logger.debug("Workflow retry. Current status=" + workflow.getStatus() + ",workflowId=" + workflow.getWorkflowId()
			+ ",correlationId=" + workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId()
			+ ",contextUser=" + workflow.getContextUser() + ",clientId=" + workflow.getClientId());
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
			logger.debug("Workflow has already been completed. Current status=" + workflow.getStatus()
				+ ", workflowId=" + wf.getWorkflowId() + ",correlationId=" + wf.getCorrelationId()
				+ ",traceId=" + workflow.getTraceId()+ ",contextUser=" + workflow.getContextUser()
				+ ",clientId=" + workflow.getClientId());
			return;
		}

		if (workflow.getStatus().isTerminal()) {
			String msg = "Workflow has already been completed. Current status " + workflow.getStatus();
			logger.debug("Workflow has already been completed. status=" + workflow.getStatus()
				+ ",workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId()
				+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
				+ ",clientId=" + workflow.getClientId());
			throw new ApplicationException(Code.CONFLICT, msg);
		}

		workflow.setStatus(WorkflowStatus.COMPLETED);
		workflow.setOutput(wf.getOutput());
		edao.updateWorkflow(workflow);
		queue.remove(deciderQueue, workflow.getWorkflowId());	//remove from the sweep queue
		queue.remove(sweeperQueue, workflow.getWorkflowId());

		// If the following task, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			wakeUpSweeper(workflow.getParentWorkflowId());
		}
		MetricService.getInstance().workflowComplete(workflow.getWorkflowType(), workflow.getStartTime());

		// send wf end message
		workflowStatusListener.onWorkflowCompleted(workflow);

		logger.debug("Workflow has completed, workflowId=" + wf.getWorkflowId()
			+ ",correlationId=" + wf.getCorrelationId() + ",traceId=" + wf.getTraceId()
			+ ",contextUser=" + workflow.getContextUser() + ",clientId=" + workflow.getClientId());
	}

	public void forceCompleteWorkflow(String workflowId, String reason) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		if (workflow.getStatus() == WorkflowStatus.COMPLETED) {
			throw new ApplicationException(Code.CONFLICT, "Workflow "+workflowId+" has already been completed");
		}

		workflow.setStatus(WorkflowStatus.COMPLETED);
		edao.updateWorkflow(workflow);
		logger.debug("Workflow is force completed. workflowId=" + workflowId + ",correlationId=" + workflow.getCorrelationId()
			+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
			+ ",clientId=" + workflow.getClientId());
		cancelTasks(workflow, workflow.getTasks(), reason);

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			wakeUpSweeper(workflow.getParentWorkflowId());
		}

		//remove from the sweep queue
		queue.remove(deciderQueue, workflowId);
		queue.remove(sweeperQueue, workflowId);

		// metrics
		MetricService.getInstance().workflowForceComplete(workflow.getWorkflowType());

		// send wf end message
		workflowStatusListener.onWorkflowCompleted(workflow);

		logger.debug("Workflow has force completed, workflowId=" + workflow.getWorkflowId()
			+ ",correlationId=" + workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId()
			+ ",contextUser=" + workflow.getContextUser() + ",clientId=" + workflow.getClientId());
	}

	public String cancelWorkflow(String workflowId , String reason) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		if (workflow.getStatus().isTerminal() || workflow.getStatus().equals(WorkflowStatus.CANCELLED) ) {
			String msg = "Workflow "+workflow.getWorkflowId()+" can not be cancelled because its already "+workflow.getStatus() ;
			logger.debug("Workflow can not be cancelled because its already " + workflow.getStatus()
				+ ",workflowId="+workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId()
				+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
				+ ",clientId=" + workflow.getClientId());
			throw new ApplicationException(Code.CONFLICT, msg);
		}
		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.CANCELLED);
		}

		workflow.setCancelledBy("API");
		return cancelWorkflow(workflow, reason);
	}

	public String cancelWorkflow(Workflow workflow, String reason) throws Exception {
		String workflowId = workflow.getWorkflowId();

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.CANCELLED);
		}

		if(StringUtils.isNotEmpty(reason)) {
			workflow.setReasonForIncompletion(reason);
		}

		edao.updateWorkflow(workflow);
		logger.debug("Workflow is cancelled. workflowId=" + workflowId + ",correlationId=" + workflow.getCorrelationId()
			+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
			+ ",clientId=" + workflow.getClientId());
		cancelTasks(workflow, workflow.getTasks(), reason);

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			wakeUpSweeper(workflow.getParentWorkflowId());
		}

		WorkflowDef def = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
		String cancelWorkflow = def.getCancelWorkflow();
		if (StringUtils.isNotEmpty(cancelWorkflow)) {
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
			input.put("contextUser", workflow.getContextUser());
			input.put("clientId", workflow.getClientId());
			input.put("cancelledBy", workflow.getCancelledBy());
			input.put("correlationId", workflow.getCorrelationId());
			input.put("reason", reason);


			try {

				WorkflowDef latestCancelWorkflow = metadata.getLatest(cancelWorkflow);
				String cancelWFId = startWorkflow(cancelWorkflow, latestCancelWorkflow.getVersion(), input,
					workflow.getCorrelationId(), workflow.getWorkflowId(), null, null, null,
					workflow.getWorkflowIds(), workflow.getTraceId());

				workflow.getOutput().put("conductor.cancel_workflow", cancelWFId);

			} catch (Exception e) {
				logger.info("Error workflow " + cancelWorkflow + " failed to start.  reason: " + e.getMessage());
				workflow.getOutput().put("conductor.cancel_workflow", "Error workflow " + cancelWorkflow + " failed to start.  reason: " + e.getMessage());
				MetricService.getInstance().workflowStartFailed(cancelWorkflow);
			}
		}

		//remove from the sweep queue
		queue.remove(deciderQueue, workflowId);
		queue.remove(sweeperQueue, workflowId);

		// metrics
		MetricService.getInstance().workflowCancel(workflow.getWorkflowType());

		// send wf end message
		workflowStatusListener.onWorkflowTerminated(workflow);

		logger.debug("Workflow has cancelled, workflowId=" + workflow.getWorkflowId()
			+ ",correlationId=" + workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId()
			+ ",contextUser=" + workflow.getContextUser() + ",clientId=" + workflow.getClientId());
		return workflowId;
	}

	public String reset(String workflowId, String reason) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.RESET);
		}

		if(StringUtils.isNotEmpty(reason)) {
			workflow.setReasonForIncompletion(reason);
		}

		edao.updateWorkflow(workflow);
		logger.debug("Workflow has been reset. workflowId=" + workflowId + ",correlationId=" + workflow.getCorrelationId()
			+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
			+ ",clientId=" + workflow.getClientId());
		cancelTasks(workflow, workflow.getTasks(), null);

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			wakeUpSweeper(workflow.getParentWorkflowId());
		}

		//remove from the sweep queue
		queue.remove(deciderQueue, workflow.getWorkflowId());
		queue.remove(sweeperQueue, workflow.getWorkflowId());

		// metrics
		MetricService.getInstance().workflowReset(workflow.getWorkflowType());

		// send wf end message
		workflowStatusListener.onWorkflowTerminated(workflow);

		return workflowId;
	}

	public void terminateWorkflow(String workflowId, String reason) throws Exception {
		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow.getStatus().isTerminal()) {
			logger.info("Workflow already finished. status=" + workflow.getStatus()
				+ ",workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId()
				+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
				+ ",clientId=" + workflow.getClientId());
			throw new ApplicationException(Code.CONFLICT, "Workflow "+workflow.getWorkflowId()+" already finished. status=" + workflow.getStatus());
		}

		terminateWorkflow(workflow, reason, null, null);
	}

	public void terminateWorkflow(Workflow workflow, String reason, String failureWorkflow, Task failedTask) throws Exception {
		String workflowId = workflow.getWorkflowId();

		if (!workflow.getStatus().isTerminal()) {
			workflow.setStatus(WorkflowStatus.TERMINATED);
		}

		if (StringUtils.isNotEmpty(reason)) {
			workflow.setReasonForIncompletion(reason);
		}

		Object originalFailed = null;
		if (failedTask != null) {
			originalFailed = failedTask.getOutputData().get("originalFailedTask");
			if (originalFailed == null) {
				Map<String, Object> map = new HashMap<>();
				map.put("input", failedTask.getInputData());
				map.put("output", failedTask.getOutputData());
				map.put("taskId", failedTask.getTaskId());
				map.put("retryCount", failedTask.getRetryCount());
				map.put("referenceName", failedTask.getReferenceTaskName());
				map.put("failureStatus", failedTask.getStatus());
				map.put("reasonForIncompletion", failedTask.getReasonForIncompletion());
				map.put("workflowId", workflowId);
				map.put("workflowType", workflow.getWorkflowType());
				map.put("workflowVersion", workflow.getVersion());
				map.put("workflowRestartCount", workflow.getRestartCount());
				map.put("workflowRerunCount", workflow.getRerunCount());
				map.put("attributes", workflow.getAttributes());
				originalFailed = map;
			}
			workflow.getOutput().put("originalFailedTask", originalFailed);
			workflow.setCancelledBy((String)failedTask.getOutputData().get("cancelledBy"));
		}

		edao.updateWorkflow(workflow);
		String message = "Workflow is terminated/reset. workflowId=" + workflowId
				+ ",correlationId=" + workflow.getCorrelationId()
				+ ",traceId=" + workflow.getTraceId()
				+ ",reasonForIncompletion=" + reason
				+ ",contextUser=" + workflow.getContextUser()
				+ ",clientId=" + workflow.getClientId();
		if (failedTask != null) {
			message += ",taskId=" + failedTask.getTaskId()
					+ ",taskRefName=" + failedTask.getReferenceTaskName()
					+ ",taskReasonForIncompletion=" + failedTask.getReasonForIncompletion();
		}
		if (WorkflowStatus.FAILED.equals(workflow.getStatus()) || WorkflowStatus.TERMINATED.equals(workflow.getStatus())) {
			logger.error(message);
		} else {
			logger.debug(message);
		}
		List<Task> tasks = workflow.getTasks();
		cancelTasks(workflow, tasks, null);

		logger.debug("Removing decider record for " + workflow.getWorkflowId());
		queue.remove(deciderQueue, workflow.getWorkflowId());	//remove from the sweep queue
		queue.remove(sweeperQueue, workflow.getWorkflowId());

		// If the following lines, for some reason fails, the sweep will take
		// care of this again!
		if (workflow.getParentWorkflowId() != null) {
			wakeUpSweeper(workflow.getParentWorkflowId());
		}

		// Handle task timeout
		if (failedTask != null && Status.TIMED_OUT.equals(failedTask.getStatus())) {
			WorkflowTask workflowTask = failedTask.getWorkflowTask();
			if (workflowTask != null
				&& workflowTask.getTimeOutWorkflow() != null
				&& StringUtils.isNotEmpty(workflowTask.getTimeOutWorkflow().getName())) {
				//
				String workflowName = workflowTask.getTimeOutWorkflow().getName();
				int workflowVersion;
				if (workflowTask.getTimeOutWorkflow().getVersion() == null) {
					WorkflowDef subFlowDef = metadata.getLatest(workflowName);
					workflowVersion = subFlowDef.getVersion();
				} else {
					workflowVersion = (int) workflowTask.getTimeOutWorkflow().getVersion();
				}

				Map<String, Object> input = new HashMap<>();
				input.put("workflowId", workflow.getWorkflowId());
				input.put("workflowType", workflow.getWorkflowType());
				input.put("correlationId", workflow.getCorrelationId());
				input.put("reason", reason);
				input.put("workflowInput", workflow.getInput());
				input.put("workflowVersion", workflow.getVersion());
				input.put("contextUser", workflow.getContextUser());
				input.put("clientId", workflow.getClientId());
				input.put("restartCount", workflow.getRestartCount());
				input.put("rerunCount", workflow.getRerunCount());
				input.put("taskId", failedTask.getTaskId());
				input.put("taskInput", failedTask.getInputData());
				input.put("taskRefName", failedTask.getReferenceTaskName());
				input.put("taskRetryCount", failedTask.getRetryCount());

				try {
					startWorkflow(workflowName, workflowVersion, input, workflow.getCorrelationId(),
						workflow.getWorkflowId(), null, null, null,
						workflow.getWorkflowIds(), workflow.getTraceId());
				} catch (Exception e) {
					logger.info("Error workflow " + workflowName + " failed to start. reason: " + e.getMessage(), e);
					MetricService.getInstance().workflowStartFailed(workflowName);
				}
			}
		}

		// Start failure workflow
		if (StringUtils.isNotEmpty(failureWorkflow) && !WorkflowStatus.TIMED_OUT.equals(workflow.getStatus())) {
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
			input.put("contextUser", workflow.getContextUser());
			input.put("clientId", workflow.getClientId());
			input.put("cancelledBy", workflow.getCancelledBy());
			input.put("correlationId", workflow.getCorrelationId());
			input.put("reason", reason);
			input.put("failureStatus", workflow.getStatus().toString());
			input.put("restartCount", workflow.getRestartCount());
			input.put("rerunCount", workflow.getRerunCount());
			if (failedTask != null) {
				Map<String, Object> map = new HashMap<>();
				map.put("taskId", failedTask.getTaskId());
				map.put("input", failedTask.getInputData());
				map.put("output", failedTask.getOutputData());
				map.put("retryCount", failedTask.getRetryCount());
				map.put("referenceName", failedTask.getReferenceTaskName());
				map.put("reasonForIncompletion", failedTask.getReasonForIncompletion());
				input.put("failedTask", map);
			}
			// originalFailed represents the task in the first failed workflow
			input.put("originalFailedTask", originalFailed);

			try {

				WorkflowDef latestFailureWorkflow = metadata.getLatest(failureWorkflow);
				String failureWFId = startWorkflow(failureWorkflow, latestFailureWorkflow.getVersion(), input,
					workflow.getCorrelationId(), workflow.getWorkflowId(), null, null,null,
					workflow.getWorkflowIds(), workflow.getTraceId());
				workflow.getOutput().put("conductor.failure_workflow", failureWFId);

			} catch (Exception e) {
				logger.info("Error workflow " + failureWorkflow + " failed to start.  reason: " + e.getMessage());
				workflow.getOutput().put("conductor.failure_workflow", "Error workflow " + failureWorkflow + " failed to start.  reason: " + e.getMessage());
				MetricService.getInstance().workflowStartFailed(failureWorkflow);
			}
		}

		// send wf end message
		workflowStatusListener.onWorkflowTerminated(workflow);

		int errorId = 0 ;
		try {
			Optional<ErrorLookup> errorLookupOpt = errorLookupDAO.getErrorMatching(workflow.getWorkflowType(), reason).stream().findFirst();
			if (errorLookupOpt.isPresent()) {
				ErrorLookup errorLookup = errorLookupOpt.get();
				errorId = errorLookup.getId();
			}
		} catch (Exception ex) {

		}

		WorkflowErrorRegistry workflowErrorRegistry = new WorkflowErrorRegistry();
		workflowErrorRegistry.setStatus(workflow.getStatus().name());
		workflowErrorRegistry.setWorkflowId(workflow.getWorkflowId());
		workflowErrorRegistry.setWorkflowType(workflow.getWorkflowType());
		workflowErrorRegistry.setErrorLookUpId(errorId);
		workflowErrorRegistry.setStartTime(workflow.getStartTime());
		workflowErrorRegistry.setEndTime(workflow.getEndTime());
		workflowErrorRegistry.setParentWorkflowId(workflow.getParentWorkflowId());
		edao.addErrorRegistry(workflowErrorRegistry);

		// Send to data dog
		MetricService.getInstance().workflowFailure(workflow.getWorkflowType(),
			workflow.getStatus().name(),
			workflow.getStartTime());
	}


	public QueueDAO getQueueDao() {
		return queue;
	}

	public ExecutionDAO getExecutionDao() {
		return edao;
	}

	public void updateTask(Task task)  {
		edao.updateTask(task);
	}

	public void updateTask(TaskResult result) throws Exception {
		if (result == null) {
			logger.debug("null task given for update..." + result);
			throw new ApplicationException(Code.INVALID_INPUT, "Task object is null");
		}
		String workflowId = result.getWorkflowInstanceId();
		Workflow wf = edao.getWorkflow(workflowId, false);
		if (wf == null) {
			logger.debug("No workflow found for " + workflowId);
			return;
		}

		Task task = edao.getTask(result.getTaskId());
		if (task == null) {
			logger.debug("No task found for " + result.getTaskId() + " in " + wf);
			return;
		}

		if (wf.getStatus().isTerminal()) {
			// Workflow is in terminal state
			queue.remove(deciderQueue, wf.getWorkflowId());	//remove from the sweep queue
			queue.remove(sweeperQueue, wf.getWorkflowId());
			queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
			if(!task.getStatus().isTerminal()) {
				task.setStatus(Status.COMPLETED);
			}
			String msg = "Workflow " + wf.getWorkflowId() + " is already completed as " + wf.getStatus() + ", task=" + task.getTaskType() + ",reason=" + wf.getReasonForIncompletion()+",correlationId="+wf.getCorrelationId() + ",contextUser=" + wf.getContextUser()+ ",clientId=" + wf.getClientId();
			logger.debug(msg);
			//Monitors.recordUpdateConflict(task.getTaskType(), wf.getWorkflowType(), wf.getStatus());
			return;
		}

		if (task.getStatus().isTerminal()) {
			// Task was already updated....
			queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
			String msg = "Task is already completed as " + task.getStatus() + "@" + task.getEndTime() + ", workflow status=" + wf.getStatus() + ",workflowId=" + wf.getWorkflowId() + ",taskId=" + task.getTaskId()+",correlationId="+wf.getCorrelationId() + ",contextUser=" + wf.getContextUser()+ ",clientId=" + wf.getClientId();
			logger.debug(msg);
			//Monitors.recordUpdateConflict(task.getTaskType(), wf.getWorkflowType(), task.getStatus());
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

		if (result.isResetStartTime()) {
			edao.resetStartTime(task, result.isUpdateOutput());
		} else {
			edao.updateTask(task);
		}
		if (task.isTerminal()) {
			MetricService.getInstance().taskComplete(task.getTaskType(),
				task.getReferenceTaskName(),
				task.getTaskDefName(),
				task.getStatus().name(),
				task.getStartTime());
		}

		result.getLogs().forEach(tl -> tl.setTaskId(task.getTaskId()));
		edao.addTaskExecLog(result.getLogs());

		switch (task.getStatus()) {

			case COMPLETED:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				taskStatusListener.onTaskFinished(task);
				break;

			case CANCELED:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				taskStatusListener.onTaskFinished(task);
				break;
			case FAILED:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				taskStatusListener.onTaskFinished(task);
				break;
			case RESET:
				queue.remove(QueueUtils.getQueueName(task), result.getTaskId());
				taskStatusListener.onTaskFinished(task);
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

		// Exit if resetStartTime was requested as decider won't do any actions
		if (result.isResetStartTime()) {
			return;
		}

		// Who calls decider ? Sweeper or current thread?
		if (lazyDecider) {
			wakeUpSweeper(workflowId);
		} else {
			decide(workflowId);
		}
	}

	private void wakeUpSweeper(String workflowId) {
		boolean active = queue.exists(WorkflowExecutor.sweeperQueue, workflowId);
		if (active) {
			logger.debug("wakeUpSweeper. Sweeper in progress for " + workflowId);

			// Wait a little bit. If the current sweeper call is empty
			// then it will exit soon and we can wake it up again
			Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

			// Check again
			active = queue.exists(WorkflowExecutor.sweeperQueue, workflowId);

			// Exit if it still active
			if (active) {
				logger.debug("wakeUpSweeper. Sweeper still in progress for " + workflowId);
				return;
			}
		}

		// Otherwise wake it up by unacking message via queue
		boolean result = queue.wakeup(WorkflowExecutor.deciderQueue, workflowId);
		logger.debug("wakeUpSweeper " + result + " for " + workflowId);
	}

	public List<Task> getTasks(String taskType, String startKey, int count) throws Exception {
		return edao.getTasks(taskType, startKey, count);
	}

	public List<Task> getPendingSystemTasks(String taskType) throws Exception {
		return edao.getPendingSystemTasks(taskType);
	}

	public List<Task> getPendingTasksByTags(String taskType, Set<String> tags) throws Exception {
		return edao.getPendingTasksByTags(taskType, tags);
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
	public Pair<Boolean, Integer> decide(String workflowId) throws Exception {
		logger.debug("Invoked decide for workflow " + workflowId);
		if (workflowId == null || workflowId.isEmpty()) {
			logger.error("ONECOND-1106: Invoked decide() with an empty or null Workflow ID");
			return Pair.of(false, config.getSweepFrequency());
		}

		Workflow workflow = edao.getWorkflow(workflowId, true);
		if (workflow == null) {
			logger.error("ONECOND-1106: getWorkflow() returned null for workflow: " + workflowId);
			return Pair.of(false, config.getSweepFrequency());
		}

		if (workflow.getStatus().isTerminal()) {
			logger.debug("Invoked decide for finished workflow " + workflowId);
			return Pair.of(true, config.getSweepFrequency());
		}

		WorkflowDef def = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
		int sweepFrequency = def.getSweepFrequency() != null && def.getSweepFrequency() > 0 ? def.getSweepFrequency() : config.getSweepFrequency();
		try {
			DeciderOutcome outcome = decider.decide(workflow, def);
			if(outcome.isComplete) {
				completeWorkflow(workflow);
				return Pair.of(true, sweepFrequency);
			}

			List<Task> tasksToBeScheduled = outcome.tasksToBeScheduled;
			setTaskDomains(tasksToBeScheduled, workflow);
			List<Task> tasksToBeUpdated = outcome.tasksToBeUpdated;
			boolean stateChanged = false;

			workflow.getTasks().addAll(tasksToBeScheduled);
			for (Task task : tasksToBeScheduled) {
				if (SystemTaskType.is(task.getTaskType()) && !task.getStatus().isTerminal()) {
					WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
					if (!stt.isAsync() && stt.execute(workflow, task, this)) {
						tasksToBeUpdated.add(task);
						stateChanged = true;
					}
				}
			}
			if (!outcome.tasksToBeDeleted.isEmpty()) {
				outcome.tasksToBeDeleted.forEach(task -> edao.removeTask(task));
			}

			stateChanged = scheduleTask(workflow, tasksToBeScheduled) || stateChanged;

			if(!outcome.tasksToBeUpdated.isEmpty() || !outcome.tasksToBeScheduled.isEmpty()) {
				edao.updateTasks(tasksToBeUpdated);
				boolean shallResetTags = tasksToBeUpdated.stream().anyMatch(t -> t.isTerminal() && t.shallResetTags());
				if (shallResetTags) {
					workflow.setResetTags(true);
				}
				edao.updateWorkflow(workflow);
				queue.push(deciderQueue, workflow.getWorkflowId(), sweepFrequency);
			}

			if(stateChanged) {
				decide(workflowId);
			}

		} catch (TerminateWorkflow tw) {
			String message = "Error in workflow execution: " + tw.getMessage()
					+ ",workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId()
					+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
					+ ",clientId=" + workflow.getClientId();
			if (tw.task != null) {
				message += ",taskId=" + tw.task.getTaskId() + ",taskRefName=" + tw.task.getReferenceTaskName();
			}
			if (WorkflowStatus.FAILED.equals(tw.workflowStatus) || WorkflowStatus.TERMINATED.equals(tw.workflowStatus)) {
				logger.error(message, tw);
			} else {
				logger.debug(message, tw);
			}
			terminate(def, workflow, tw);
			return Pair.of(true, sweepFrequency);
		}
		return Pair.of(false, sweepFrequency);
	}

	public void pauseWorkflow(String workflowId,String correlationId) throws Exception {
		WorkflowStatus status = WorkflowStatus.PAUSED;
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

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
		// metrics
		MetricService.getInstance().workflowPause(workflow.getWorkflowType());
	}

	public void resumeWorkflow(String workflowId,String correlationId) throws Exception{
		Workflow workflow = edao.getWorkflow(workflowId, false);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);

		if(!workflow.getStatus().equals(WorkflowStatus.PAUSED)){
			logger.info("Workflow is not is not PAUSED so cannot resume. Current status=" + workflow.getStatus()
				+ ",workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId()
				+ ",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser()
				+ ",clientId=" + workflow.getClientId());
			throw new IllegalStateException("The workflow " + workflowId + " is not is not PAUSED so cannot resume");
		}
		workflow.setStatus(WorkflowStatus.RUNNING);
		if(StringUtils.isNotEmpty(correlationId)) {
			workflow.setCorrelationId(correlationId);
		}
		edao.updateWorkflow(workflow);
		// metrics
		MetricService.getInstance().workflowResume(workflow.getWorkflowType());
		decide(workflowId);
	}

	public void skipTaskFromWorkflow(String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest)  throws Exception {

		Workflow wf = edao.getWorkflow(workflowId, true);

		// If the wf is not running then cannot skip any task
		if(!wf.getStatus().equals(WorkflowStatus.RUNNING)){
			String errorMsg = String.format("The workflow %s is not running so the task referenced by %s cannot be skipped", workflowId, taskReferenceName);
			logger.info(errorMsg);
			throw new IllegalStateException(errorMsg);
		}
		// Check if the reference name is as per the workflowdef
		WorkflowDef wfd = metadata.get(wf.getWorkflowType(), wf.getVersion());
		WorkflowTask wft = wfd.getTaskByRefName(taskReferenceName);
		if(wft == null){
			String errorMsg = String.format("The task referenced by %s does not exist in the WorkflowDef %s", taskReferenceName, wf.getWorkflowType());
			logger.info(errorMsg);
			throw new IllegalStateException(errorMsg);
		}
		// If the task is already started the again it cannot be skipped
		wf.getTasks().forEach(task -> {
			if(task.getReferenceTaskName().equals(taskReferenceName)){
				String errorMsg = String.format("The task referenced %s has already been processed, cannot be skipped", taskReferenceName);
				logger.info(errorMsg);
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

	public Task getTask(String workflowId, String taskRefName) {
		return edao.getTask(workflowId, taskRefName);
	}

	public void addTaskToQueue(Task task) throws Exception {
		// put in queue
		queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
		pushTaskToQueue(task);
	}

	public void pushTaskToQueue(Task task) throws Exception {
		if (task.getCallbackAfterSeconds() > 0) {
			queue.push(QueueUtils.getQueueName(task), task.getTaskId(), task.getCallbackAfterSeconds());
		} else {
			queue.push(QueueUtils.getQueueName(task), task.getTaskId(), 0);
		}
	}

	public void removeWorkflow(String workflowId) {
		Workflow workflow = getWorkflow(workflowId, false);
		if (workflow == null)
			throw new ApplicationException(Code.NOT_FOUND, "No workflow found with id " + workflowId);
		edao.removeWorkflow(workflowId);
		// metrics
		MetricService.getInstance().workflowRemove(workflow.getWorkflowType());
	}

	public void removeWorkflowNotImplemented(String workflowId) {
		throw new ApplicationException(Code.NOT_IMPLEMENTED, "Method not implemented");
	}

	//Executes the async system task
	public void executeSystemTask(WorkflowSystemTask systemTask, String taskId, int callbackSeconds) {
		try {
			logger.debug("Executing async taskId={}, callbackSeconds={}, retryTimeIn={}", taskId, callbackSeconds, systemTask.getRetryTimeInSecond());

			Task task = edao.getTask(taskId);
			if (task == null) {
				logger.debug("No task found for task id = " + taskId + ". System task is " + systemTask);
				return;
			}
			String queueName = QueueUtils.getQueueName(task);

			if (task.getStatus().isTerminal()) {
				//Tune the SystemTaskWorkerCoordinator's queues - if the queue size is very big this can happen!
				logger.debug("Task {}/{} was already completed.", task.getTaskType(), task.getTaskId());
				queue.remove(queueName, task.getTaskId());
				return;
			}

			String workflowId = task.getWorkflowInstanceId();
			Workflow workflow = edao.getWorkflow(workflowId, true);

			if (workflow.getStatus().isTerminal()) {
				logger.debug("Workflow {} has been completed for {}/{}", workflow.getWorkflowId(), systemTask.getName(), task.getTaskId());
				if (!task.getStatus().isTerminal()) {
					task.setStatus(Status.CANCELED);
				}
				edao.updateTask(task);
				queue.remove(queueName, task.getTaskId());
				taskStatusListener.onTaskFinished(task);
				return;
			}

			if (task.getStatus().equals(Status.SCHEDULED)) {
				String propName = "workflow.system.task." + task.getTaskDefName().toLowerCase() + ".unpop.offset";
				int unpopOffset = config.getIntProperty(propName, 30);

				if (edao.exceedsInProgressLimit(task)) {
					MetricService.getInstance().taskRateLimited(task.getTaskType(), task.getReferenceTaskName(), task.getTaskDefName());
					logger.debug("Concurrent Execution limited for {}:{}:{}", task.getReferenceTaskName(), task.getTaskDefName(), taskId);
					queue.unpop(queueName, task.getTaskId(), unpopOffset * 1000L);
					return;
				}

				if (edao.exceedsRateLimitPerFrequency(task)) {
					MetricService.getInstance().taskRateLimited(task.getTaskType(), task.getReferenceTaskName(), task.getTaskDefName());
					logger.debug("RateLimit Execution limited for {}:{}:{}", task.getReferenceTaskName(), task.getTaskDefName(), taskId);
					queue.unpop(queueName, task.getTaskId(), unpopOffset * 1000L);
					return;
				}
			}

			// Workaround when workflow id disappears from the queue
			boolean exists = queue.exists(WorkflowExecutor.deciderQueue, workflowId);
			if (!exists) {
				// If not exists then need place back
				queue.pushIfNotExists(WorkflowExecutor.deciderQueue, workflowId, config.getSweepFrequency());
			}

			logger.debug("Executing {}/{}-{} for workflowId={},correlationId={},traceId={},contextUser={},clientId={}",
				task.getTaskType(), task.getReferenceTaskName(), task.getTaskId(), workflow.getWorkflowId(),
				workflow.getCorrelationId(), workflow.getTraceId(), workflow.getContextUser(), workflow.getClientId());

			String propName = "workflow.system.task." + task.getTaskDefName().toLowerCase() + ".unack.timeout";
			int unackTimeout = config.getIntProperty(propName, systemTask.getRetryTimeInSecond());

			queue.setUnackTimeout(queueName, task.getTaskId(), unackTimeout * 1000L);
			task.setStarted(true);
			if (task.getStartTime() == 0) {
				task.setStartTime(System.currentTimeMillis());
			}
			task.setPollCount(task.getPollCount() + 1);
			edao.updateTask(task);

			// Metrics
			MetricService.getInstance().taskWait(task.getTaskType(),
				task.getReferenceTaskName(),
				task.getTaskDefName(),
				task.getQueueWaitTime());

			switch (task.getStatus()) {

				case SCHEDULED:
					try {
						edao.updateInProgressStatus(task);
						taskStatusListener.onTaskStarted(task);
						systemTask.start(workflow, task, this);
					} catch (Exception ex) {
						task.setStatus(Status.FAILED);
						task.setReasonForIncompletion(ex.getMessage());
						logger.info("Task {}/{} failed with Exception {}.", task.getTaskType(), task.getTaskId(),ex.getMessage());
					}
					break;

				case IN_PROGRESS:
					systemTask.execute(workflow, task, this);
					break;
				default:
					break;
			}

			if (!task.getStatus().isTerminal()) {
				task.setCallbackAfterSeconds(callbackSeconds);
			}

			updateTask(new TaskResult(task));
			logger.debug("Done Executing {}/{}-{} for workflowId={},correlationId={},traceId={},contextUser={},clientId={}",
				task.getTaskType(), task.getReferenceTaskName(), task.getTaskId(), workflow.getWorkflowId(), workflow.getCorrelationId(),
				workflow.getTraceId(), workflow.getContextUser(), workflow.getClientId());

		} catch (Exception e) {
			logger.info("ExecuteSystemTask failed with " + e.getMessage() + " for task id=" + taskId + ", system task=" + systemTask, e);
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

		// Might be null due to auto-cleanup
		Task retriedTask = edao.getTask(task.getRetriedTaskId());
		if (retriedTask == null) {
			return s;
		}
		return s + getTaskDuration(s, retriedTask);
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

		// Tasks had to be started at previous scheduleTask call
		List<Task> stuckSystemTasks = tasks.stream().filter(task -> SystemTaskType.is(task.getTaskType())
			&& !created.contains(task)
			&& task.isStarted() != null // The legacy tasks which did not have started attribute
			&& !task.isStarted()
		).collect(Collectors.toList());
		boolean startedSystemTasks = false;

		// We need start those stuck tasks first
		for (Task task : stuckSystemTasks) {
			String lockQueue = QueueUtils.getQueueName(task) + ".lock";
			WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
			if (stt == null) {
				throw new RuntimeException("No system task found by name " + task.getTaskType());
			}

			// This prevents another containers executing the same action
			// true means this session added the record to lock queue and can start the task
			boolean locked = queue.pushIfNotExists(lockQueue, task.getTaskId(), 600); // 10 minutes

			// This session couldn't lock the task (cluster pooling)
			if (!locked) {
				logger.debug("skipping processing of stuck task " + task +
					".workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId() +
					",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser());
				continue;
			}

			try {
				if (stt.isAsync()) {
					// Async task id exists in task queue - not the stuck task
					boolean exists = queue.exists(QueueUtils.getQueueName(task), task.getTaskId());
					if (!exists) {
						logger.debug("queueing stuck task " + task +
							".workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId() +
							",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser());
						pushTaskToQueue(task);
					}
				} else {
					logger.debug("starting stuck task " + task +
						".workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId() +
						",traceId=" + workflow.getTraceId() + ",contextUser=" + workflow.getContextUser());

					startTask(stt, workflow, task);
					startedSystemTasks = true;
				}
			} finally {
				queue.remove(lockQueue, task.getTaskId());
			}
		}

		// Start the rest of the tasks
		for (Task task : createdSystemTasks) {
			WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
			if (stt == null) {
				throw new RuntimeException("No system task found by name " + task.getTaskType());
			}
			if (!stt.isAsync()) {
				startTask(stt, workflow, task);
				startedSystemTasks = true;
			} else {
				toBeQueued.add(task);
			}
		}
		addTaskToQueue(toBeQueued);
		return startedSystemTasks;
	}

	private void startTask(WorkflowSystemTask stt, Workflow workflow, Task task) throws Exception {
		if (task.getStartTime() == 0) {
			task.setStartTime(System.currentTimeMillis());
		}
		taskStatusListener.onTaskStarted(task);
		stt.start(workflow, task, this);
		task.setStarted(true);
		edao.updateTask(task);
		if (task.getStatus().isTerminal()) {
			taskStatusListener.onTaskFinished(task);
		}
	}

	private void addTaskToQueue(final List<Task> tasks) throws Exception {
		for (Task t : tasks) {
			pushTaskToQueue(t);
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
		String message = "Workflow failed/reset. workflowId=" + workflow.getWorkflowId()
			+ ",correlationId=" + workflow.getCorrelationId() + ",traceId=" + workflow.getTraceId() + ",reason=" + tw.getMessage()
			+ ",taskId=" + taskId + ",taskReferenceName=" + taskRefName
			+ ",contextUser=" + workflow.getContextUser()
			+ ",clientId=" + workflow.getClientId();
		if (WorkflowStatus.FAILED.equals(tw.workflowStatus) || WorkflowStatus.TERMINATED.equals(tw.workflowStatus)) {
			logger.error(message);
		} else {
			logger.debug(message);
		}
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

	public String checkUserRoles(HttpHeaders headers) {
		List<String> strings = headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
		String primary_role = "";
		if (strings == null || strings.isEmpty())
			throw new ApplicationException(Code.UNAUTHORIZED, "No " + HttpHeaders.AUTHORIZATION + " header provided");
		try {
			String header = strings.get(0);
			if (StringUtils.isEmpty(header))
				throw new ApplicationException(Code.UNAUTHORIZED, HttpHeaders.AUTHORIZATION + " header is empty");

			if (header.length() <= BEARER.length())
				throw new ApplicationException(Code.UNAUTHORIZED, HttpHeaders.AUTHORIZATION + " header too short");

			String type = header.substring(0, BEARER.length());
			String token = header.substring(BEARER.length() + 1);

			// Checking bearer format
			if (!BEARER.equalsIgnoreCase(type))
				throw new ApplicationException(Code.UNAUTHORIZED, "Invalid " + HttpHeaders.AUTHORIZATION + " type(" + type + ")");

			Map<String, Object> decoded = auth.decode(token);
			Map<String, Object> resource_access = (Map<String, Object>) decoded.get("resource_access");
			Map<String, Object> conductor_role = (Map<String, Object>) resource_access.get("deluxe.conductor");
			List<String> conductor_roles = null;
			List<String> conductor_roles_ui = null;
			if (conductor_role != null && !conductor_role.isEmpty()) {
				conductor_roles = (List<String>) conductor_role.get("roles");
			}
			Map<String, Object> conductor_role_ui = (Map<String, Object>) resource_access.get("deluxe.conductor-ui");
			if (conductor_role_ui != null && !conductor_role_ui.isEmpty()) {
				conductor_roles_ui = (List<String>) conductor_role_ui.get("roles");
			}
			if (conductor_roles!=null && !conductor_roles.isEmpty()) {
                if (conductor_roles.contains("deluxe.conductor.admin")) {
                    primary_role = "deluxe.conductor.admin";
                } else if (conductor_roles.contains("deluxe.conductor.viewer")) {
                    primary_role = "deluxe.conductor.viewer";
                } else if (conductor_roles.contains("deluxe.conductor.developer")) {
                    primary_role = "deluxe.conductor.viewer";
                }
            }
			if (conductor_role_ui!=null && !conductor_role_ui.isEmpty()) {
				if (conductor_roles_ui.contains("deluxe.conductor-ui.admin")) {
					return "deluxe.conductor-ui.admin";
				}
			}
			return primary_role;
		} catch (Exception ex) {
			logger.debug("checkUserRoles failed with " + ex.getMessage(), ex);
			throw new ApplicationException(Code.UNAUTHORIZED, "checkUserRoles failed: " + ex.getMessage());
		}
	}

	public Map<String, Object> validateAuth(WorkflowDef workflowDef, HttpHeaders headers) {
		if (!validateAuth || MapUtils.isEmpty(workflowDef.getAuthValidation())) {
			return null;
		}

		List<String> strings = headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
		if (strings == null || strings.isEmpty())
			throw new ApplicationException(Code.UNAUTHORIZED, "No " + HttpHeaders.AUTHORIZATION + " header provided");

		// It gives us: Bearer token
		String header = strings.get(0);
		if (StringUtils.isEmpty(header))
			throw new ApplicationException(Code.UNAUTHORIZED, "No " + HttpHeaders.AUTHORIZATION + " header provided");

		// Validate length
		if (header.length() <= BEARER.length())
			throw new ApplicationException(Code.UNAUTHORIZED, HttpHeaders.AUTHORIZATION + " header too short");

		// Get the bearer & access token
		String type = header.substring(0, BEARER.length());
		String token = header.substring(BEARER.length() + 1);

		// Checking bearer format
		if (!BEARER.equalsIgnoreCase(type))
			throw new ApplicationException(Code.UNAUTHORIZED, "Invalid " + HttpHeaders.AUTHORIZATION + " type(" + type + ")");

		Map<String, Object> decoded;

		// Do a validation
		Map<String, Object> failedList;
		try {
			decoded = auth.decode(token);
			failedList = auth.validate(decoded, workflowDef.getAuthValidation());
		} catch (Exception ex) {
			throw new ApplicationException(Code.UNAUTHORIZED, "Auth validation failed: " + ex.getMessage());
		}

		if (!failedList.isEmpty()) {
			throw new ApplicationException(Code.UNAUTHORIZED, "Auth validation rules failed: " + failedList.keySet());
		}

		return decoded;
	}

	public String decodeAuthorizationUser(HttpHeaders headers) {
		List<String> strings = headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
		if (strings == null || strings.isEmpty())
			return null;
		try {
			String header = strings.get(0);
			if (StringUtils.isEmpty(header))
				return null;

			if (header.length() <= BEARER.length())
				throw new ApplicationException(Code.UNAUTHORIZED, HttpHeaders.AUTHORIZATION + " header too short");

			String type = header.substring(0, BEARER.length());
			String token = header.substring(BEARER.length() + 1);

			// Checking bearer format
			if (!BEARER.equalsIgnoreCase(type))
				throw new ApplicationException(Code.UNAUTHORIZED, "Invalid " + HttpHeaders.AUTHORIZATION + " type(" + type + ")");

			Map<String, Object> decoded = auth.decode(token);
			String username = (String)decoded.get("preferred_username");
			String email = (String)decoded.get("email");
			return String.format("%s(%s)", username, email);
		} catch (Exception ex) {
			logger.debug("decodeAuthorizationUser failed with " + ex.getMessage(), ex);
			return null;
		}
	}

	public String validateContextUser(String contextToken) {
		// Decode auth context if passed and store at workflow level
		String contextUser = null;
		if (authContextEnabled && StringUtils.isNotEmpty(contextToken)) {
			try {
				Map<String, Object> context = auth.decode(contextToken);
				String username = (String)context.get("preferred_username");
				String email = (String)context.get("email");
				contextUser = String.format("%s(%s)", username, email);
			} catch (Exception ex) {
				throw new ApplicationException(Code.UNAUTHORIZED, "Auth context validation failed: " + ex.getMessage());
			}
		}
		return contextUser;
	}

	public Task getTask(String taskId) {
		return edao.getTask(taskId);
	}

	public void resetStartTime(String workflowId, String taskRefName) {
		Task task = edao.getTask(workflowId, taskRefName);
		if (task != null && task.getStatus() == Status.IN_PROGRESS) {
			task.setStartTime(System.currentTimeMillis());
			// We must reset endtime only when it is set
			if (task.getEndTime() > 0) {
				task.setEndTime(System.currentTimeMillis());
			}
			edao.resetStartTime(task, false);
		}
	}

	public void addEventPublished(ObservableQueue queue, Message msg) {
		try {
			Map<String, Object> payload = om.readValue(msg.getPayload(), new TypeReference<Map<String, Object>>() {
			});

			String subject = queue.getURI();
			if (queue.getURI().contains(":")) {
				subject = queue.getURI().substring(0, queue.getURI().indexOf(':'));
			}

			EventPublished ep = new EventPublished();
			ep.setId(msg.getId());
			ep.setSubject(subject);
			ep.setPayload(payload);
			ep.setType(queue.getType());
			ep.setPublished(System.currentTimeMillis());

			edao.addEventPublished(ep);
		} catch (Exception ex) {
			logger.debug("addEventPublished failed with " + ex.getMessage() +
				" for queue uri=" + queue.getURI() + ", payload=" + msg.getPayload());
		}
	}

	private void cancelTasks(Workflow workflow, List<Task> tasks, String reason) throws Exception {
		for (Task task : tasks) {
			if (!task.getStatus().isTerminal()) {
				// Cancel the ones which are not completed yet....
				task.setStatus(Status.CANCELED);
				task.setReasonForIncompletion(StringUtils.defaultIfEmpty(reason, workflow.getReasonForIncompletion()));
				if (SystemTaskType.is(task.getTaskType())) {
					WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
					stt.cancel(workflow, task, this);
					//SystemTaskType.valueOf(task.getTaskType()).cancel(workflow, task, this);
				}
				edao.updateTask(task);
				taskStatusListener.onTaskFinished(task);
			}
			// And remove from the task queue if they were there
			queue.remove(QueueUtils.getQueueName(task), task.getTaskId());
		}
	}

	private void removeQuietly(String workflowId) {
		try {
			edao.removeWorkflow(workflowId);
		} catch (Exception ignore) {
			logger.debug("WorkflowId  " + workflowId + ", failed with " + ignore.getMessage(), ignore);
		}
	}

	private void resumeParent(String workflowId, String subWorkflowId) throws Exception {
		if (StringUtils.isEmpty(workflowId)) {
			return;
		}

		// Get the workflow and find related task
		Workflow workflow = getWorkflow(workflowId, true);
		if (workflow == null) {
			logger.debug("No workflow found with id " + workflowId);
			return;
		}

		workflow.setStatus(WorkflowStatus.RUNNING);
		workflow.setReasonForIncompletion(null);
		edao.updateWorkflow(workflow);
		workflowStatusListener.onWorkflowStarted(workflow);

		Task task = workflow.getTasks().stream()
				.filter(t -> t.getTaskType().equals(SubWorkflow.NAME))
				.filter(t -> t.getInputData().get("subWorkflowId") != null)
				.filter(t -> t.getInputData().get("subWorkflowId").equals(subWorkflowId))
				.findFirst().orElse(null);

		if (task != null) {
			// Resume primary (from parameters) sub-workflow
			task.setStatus(Status.IN_PROGRESS);
			task.setStartTime(System.currentTimeMillis());
			task.setEndTime(0);
			task.setReasonForIncompletion(null);
			task.setRetried(false);
			edao.updateTask(task);
			taskStatusListener.onTaskStarted(task);

			// Find neighbor sub-workflows excluding subWorkflowId
			List<Task> subs = workflow.getTasks().stream()
					.filter(t -> t.getTaskType().equals(SubWorkflow.NAME))
					.filter(t -> t.getStatus().equals(Status.CANCELED))
					.filter(t -> t.getInputData().get("subWorkflowId") != null)
					.filter(t -> !t.getInputData().get("subWorkflowId").equals(subWorkflowId))
					.collect(Collectors.toList());

			// Expecting that Join exists in CANCELED status
			if (!subs.isEmpty()) {
				Task join = workflow.getTasks().stream()
						.filter(t -> t.getTaskType().equals("JOIN"))
						.filter(t -> t.getStatus().equals(Status.CANCELED))
						.findFirst().orElse(null);
				if (join != null) {
					join.setStatus(Status.IN_PROGRESS);
					join.setStartTime(System.currentTimeMillis());
					join.setEndTime(0);
					join.setReasonForIncompletion(null);
					join.setRetried(false);
					edao.updateTask(join);
				}
			}

			// Resume rest sub-workflow tasks and sub-workflows behind them
			subs.forEach(this::resumeSubWorkflow);
		}

		if (StringUtils.isNotEmpty(workflow.getParentWorkflowId())) {
			resumeParent(workflow.getParentWorkflowId(), workflowId);
		}
	}

	private void resumeSubWorkflow(Task task) {
		String workflowId = (String)task.getInputData().get("subWorkflowId");

		task.setStatus(Status.IN_PROGRESS);
		task.setStartTime(System.currentTimeMillis());
		task.setEndTime(0);
		task.setReasonForIncompletion(null);
		task.setRetried(false);
		edao.updateTask(task);
		taskStatusListener.onTaskStarted(task);

		Workflow subWorkflow = getWorkflow(workflowId, true);
		String cancelled = subWorkflow.getTasks().stream()
				.filter(t -> t.getStatus().equals(Status.CANCELED))
				.map(Task::getTaskId)
				.findFirst().orElse(null);
		try {
			rerunWF(workflowId, cancelled, null, null, null, null, null);
		} catch (Exception e) {
			logger.debug("workflowId" + workflowId + ", failed in resumeSubWorkflow with " + e.getMessage(), e);
			e.printStackTrace();
		}
	}
}

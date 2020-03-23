/*
 * Copyright 2019 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution;

import static com.netflix.conductor.common.metadata.tasks.Task.Status.CANCELED;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.FAILED;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.FAILED_WITH_TERMINAL_ERROR;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.IN_PROGRESS;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.SCHEDULED;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.SKIPPED;
import static com.netflix.conductor.common.metadata.tasks.Task.Status.valueOf;
import static com.netflix.conductor.common.metadata.workflow.TaskType.SUB_WORKFLOW;
import static com.netflix.conductor.common.metadata.workflow.TaskType.TERMINATE;
import static com.netflix.conductor.core.execution.ApplicationException.Code.CONFLICT;
import static com.netflix.conductor.core.execution.ApplicationException.Code.INVALID_INPUT;
import static com.netflix.conductor.core.execution.ApplicationException.Code.NOT_FOUND;
import static com.netflix.conductor.core.execution.tasks.SubWorkflow.SUB_WORKFLOW_ID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.tasks.TaskResult.Status;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.common.utils.RetryUtil;
import com.netflix.conductor.common.utils.TaskUtils;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionLockService;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Viren Workflow services provider interface
 */
@Trace
public class WorkflowExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final MetadataDAO metadataDAO;
    private final QueueDAO queueDAO;
    private final DeciderService deciderService;
    private final Configuration config;
    private final MetadataMapperService metadataMapperService;
    private final ExecutionDAOFacade executionDAOFacade;

    private WorkflowStatusListener workflowStatusListener;

    private int activeWorkerLastPollInSecs;
    public static final String DECIDER_QUEUE = "_deciderQueue";
    private static final String className = WorkflowExecutor.class.getSimpleName();
    private final ExecutionLockService executionLockService;

    @Inject
    public WorkflowExecutor(
            DeciderService deciderService,
            MetadataDAO metadataDAO,
            QueueDAO queueDAO,
            MetadataMapperService metadataMapperService,
            WorkflowStatusListener workflowStatusListener,
            ExecutionDAOFacade executionDAOFacade,
            Configuration config,
            ExecutionLockService executionLockService
    ) {
        this.deciderService = deciderService;
        this.metadataDAO = metadataDAO;
        this.queueDAO = queueDAO;
        this.config = config;
        this.metadataMapperService = metadataMapperService;
        this.executionDAOFacade = executionDAOFacade;
        this.activeWorkerLastPollInSecs = config.getIntProperty("tasks.active.worker.lastpoll", 10);
        this.workflowStatusListener = workflowStatusListener;
        this.executionLockService = executionLockService;
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(String name, Integer version, String correlationId, Map<String, Object> input, String externalInputPayloadStoragePath) {
        return startWorkflow(name, version, correlationId, input, externalInputPayloadStoragePath, null);
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(String name, Integer version, String correlationId, Integer priority, Map<String, Object> input, String externalInputPayloadStoragePath) {
        return startWorkflow(name, version, correlationId, priority, input, externalInputPayloadStoragePath, null);
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(String name, Integer version, String correlationId, Map<String, Object> input, String externalInputPayloadStoragePath, String event) {
        return startWorkflow(
                name,
                version,
                input,
                externalInputPayloadStoragePath,
                correlationId,
                null,
                null,
                event
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(String name, Integer version, String correlationId, Integer priority, Map<String, Object> input, String externalInputPayloadStoragePath, String event) {
        return startWorkflow(
                name,
                version,
                input,
                externalInputPayloadStoragePath,
                correlationId,
                priority,
                null,
                null,
                event,
                null
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            String name,
            Integer version,
            String correlationId,
            Map<String, Object> input,
            String externalInputPayloadStoragePath,
            String event,
            Map<String, String> taskToDomain
    ) {
        return startWorkflow(name, version, correlationId, 0, input, externalInputPayloadStoragePath,
                event, taskToDomain);
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            String name,
            Integer version,
            String correlationId,
            Integer priority,
            Map<String, Object> input,
            String externalInputPayloadStoragePath,
            String event,
            Map<String, String> taskToDomain
    ) {
        return startWorkflow(
                name,
                version,
                input,
                externalInputPayloadStoragePath,
                correlationId,
                priority,
                null,
                null,
                event,
                taskToDomain
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            String name,
            Integer version,
            Map<String, Object> input,
            String externalInputPayloadStoragePath,
            String correlationId,
            String parentWorkflowId,
            String parentWorkflowTaskId,
            String event
    ) {
        return startWorkflow(
                name,
                version,
                input,
                externalInputPayloadStoragePath,
                correlationId,
                parentWorkflowId,
                parentWorkflowTaskId,
                event,
                null
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            WorkflowDef workflowDefinition,
            Map<String, Object> workflowInput,
            String externalInputPayloadStoragePath,
            String correlationId,
            String event,
            Map<String, String> taskToDomain
    ) {
        return startWorkflow(
                workflowDefinition,
                workflowInput,
                externalInputPayloadStoragePath,
                correlationId,
                0,
                event,
                taskToDomain
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            WorkflowDef workflowDefinition,
            Map<String, Object> workflowInput,
            String externalInputPayloadStoragePath,
            String correlationId,
            Integer priority,
            String event,
            Map<String, String> taskToDomain
    ) {
        return startWorkflow(
                workflowDefinition,
                workflowInput,
                externalInputPayloadStoragePath,
                correlationId,
                priority,
                null,
                null,
                event,
                taskToDomain
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            String name,
            Integer version,
            Map<String, Object> workflowInput,
            String externalInputPayloadStoragePath,
            String correlationId,
            String parentWorkflowId,
            String parentWorkflowTaskId,
            String event,
            Map<String, String> taskToDomain
    ) {
        return startWorkflow(
                name,
                version,
                workflowInput,
                externalInputPayloadStoragePath,
                correlationId,
                0,
                parentWorkflowId,
                parentWorkflowTaskId,
                event,
                taskToDomain
        );
    }

    /**
     * @throws ApplicationException
     */
    public String startWorkflow(
            String name,
            Integer version,
            Map<String, Object> workflowInput,
            String externalInputPayloadStoragePath,
            String correlationId,
            Integer priority,
            String parentWorkflowId,
            String parentWorkflowTaskId,
            String event,
            Map<String, String> taskToDomain
    ) {
        WorkflowDef workflowDefinition = metadataMapperService.lookupForWorkflowDefinition(name, version);

        return startWorkflow(
                workflowDefinition,
                workflowInput,
                externalInputPayloadStoragePath,
                correlationId,
                priority,
                parentWorkflowId,
                parentWorkflowTaskId,
                event,
                taskToDomain
        );
    }

    private final Predicate<PollData> validateLastPolledTime = pd -> pd.getLastPollTime() > System.currentTimeMillis() - (activeWorkerLastPollInSecs * 1000);

    private final Predicate<Task> isSystemTask = task -> SystemTaskType.is(task.getTaskType());

    private final Predicate<Task> isNonTerminalTask = task -> !task.getStatus().isTerminal();

    /**
     * @throws ApplicationException if validation fails
     */
    public String startWorkflow(
            WorkflowDef workflowDefinition,
            Map<String, Object> workflowInput,
            String externalInputPayloadStoragePath,
            String correlationId,
            Integer priority,
            String parentWorkflowId,
            String parentWorkflowTaskId,
            String event,
            Map<String, String> taskToDomain
    ) {

        workflowDefinition = metadataMapperService.populateTaskDefinitions(workflowDefinition);

        // perform validations
        validateWorkflow(workflowDefinition, workflowInput, externalInputPayloadStoragePath);

        //A random UUID is assigned to the work flow instance
        String workflowId = IDGenerator.generate();

        // Persist the Workflow
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setCorrelationId(correlationId);
        workflow.setPriority(priority);
        workflow.setWorkflowDefinition(workflowDefinition);
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow.setParentWorkflowId(parentWorkflowId);
        workflow.setParentWorkflowTaskId(parentWorkflowTaskId);
        workflow.setOwnerApp(WorkflowContext.get().getClientApp());
        workflow.setCreateTime(System.currentTimeMillis());
        workflow.setUpdatedBy(null);
        workflow.setUpdateTime(null);
        workflow.setEvent(event);
        workflow.setTaskToDomain(taskToDomain);

        workflow.setInput(workflowInput);
        if (workflow.getInput() != null) {
            deciderService.externalizeWorkflowData(workflow);
        } else {
            workflow.setInput(null);
            workflow.setExternalInputPayloadStoragePath(externalInputPayloadStoragePath);
        }

        try {
            executionDAOFacade.createWorkflow(workflow);
            LOGGER.debug("A new instance of workflow: {} created with id: {}", workflow.getWorkflowName(), workflowId);
            //then decide to see if anything needs to be done as part of the workflow
            decide(workflowId);
            return workflowId;
        } catch (Exception e) {
            executionDAOFacade.removeWorkflow(workflowId, false);
            Monitors.recordWorkflowStartError(workflowDefinition.getName(), WorkflowContext.get().getClientApp());
            LOGGER.error("Unable to start workflow: {}", workflowDefinition.getName(), e);
            throw e;
        }
    }

    /**
     * Performs validations for starting a workflow
     *
     * @throws ApplicationException if the validation fails
     */
    private void validateWorkflow(WorkflowDef workflowDef, Map<String, Object> workflowInput, String externalStoragePath) {
        try {
            //Check if the input to the workflow is not null
            if (workflowInput == null && StringUtils.isBlank(externalStoragePath)) {
                LOGGER.error("The input for the workflow '{}' cannot be NULL", workflowDef.getName());
                throw new ApplicationException(INVALID_INPUT, "NULL input passed when starting workflow");
            }
        } catch (Exception e) {
            Monitors.recordWorkflowStartError(workflowDef.getName(), WorkflowContext.get().getClientApp());
            throw e;
        }
    }


    /**
     * @param workflowId the id of the workflow for which callbacks are to be reset
     * @throws ApplicationException if the workflow is in terminal state
     */
    public void resetCallbacksForInProgressTasks(String workflowId) {
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);
        if (workflow.getStatus().isTerminal()) {
            throw new ApplicationException(CONFLICT, "Workflow is in terminal state. Status =" + workflow.getStatus());
        }

        // Get tasks that have callbackAfterSeconds > 0 and set the callbackAfterSeconds to 0
        for (Task task : workflow.getTasks()) {
            if (task.getCallbackAfterSeconds() > 0) {
                if (queueDAO.setOffsetTime(QueueUtils.getQueueName(task), task.getTaskId(), 0)) {
                    task.setCallbackAfterSeconds(0);
                    executionDAOFacade.updateTask(task);
                }
            }
        }
    }

    public String rerun(RerunWorkflowRequest request) {
        Preconditions.checkNotNull(request.getReRunFromWorkflowId(), "reRunFromWorkflowId is missing");
        if (!rerunWF(request.getReRunFromWorkflowId(), request.getReRunFromTaskId(), request.getTaskInput(),
                request.getWorkflowInput(), request.getCorrelationId())) {
            throw new ApplicationException(INVALID_INPUT, "Task " + request.getReRunFromTaskId() + " not found");
        }
        return request.getReRunFromWorkflowId();
    }

    /**
     * @param workflowId           the id of the workflow to be restarted
     * @param useLatestDefinitions if true, use the latest workflow and task definitions upon restart
     * @throws ApplicationException in the following cases:
     *                              <ul>
     *                              <li>Workflow is not in a terminal state</li>
     *                              <li>Workflow definition is not found</li>
     *                              <li>Workflow is deemed non-restartable as per workflow definition</li>
     *                              </ul>
     */
    public void rewind(String workflowId, boolean useLatestDefinitions) {
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);
        if (!workflow.getStatus().isTerminal()) {
            String errorMsg = String.format("Workflow: %s is not in terminal state, unable to restart.", workflow);
            LOGGER.error(errorMsg);
            throw new ApplicationException(CONFLICT, errorMsg);
        }

        WorkflowDef workflowDef;
        if (useLatestDefinitions) {
            workflowDef = metadataDAO.getLatest(workflow.getWorkflowName())
                    .orElseThrow(() -> new ApplicationException(NOT_FOUND, String.format("Unable to find latest definition for %s", workflowId)));
            workflow.setVersion(workflowDef.getVersion()); // setting this here to ensure backward compatibility and consistency for workflows without the embedded workflow definition
            workflow.setWorkflowDefinition(workflowDef);
        } else {
            workflowDef = Optional.ofNullable(workflow.getWorkflowDefinition())
                    .orElseGet(() -> metadataDAO.get(workflow.getWorkflowName(), workflow.getWorkflowVersion())
                            .orElseThrow(() -> new ApplicationException(NOT_FOUND, String.format("Unable to find definition for %s", workflowId)))
                    );
        }

        if (!workflowDef.isRestartable() && workflow.getStatus().equals(WorkflowStatus.COMPLETED)) { // Can only restart non-completed workflows when the configuration is set to false
            throw new ApplicationException(CONFLICT, String.format("Workflow: %s is non-restartable", workflow));
        }

        // Reset the workflow in the primary datastore and remove from indexer; then re-create it
        executionDAOFacade.resetWorkflow(workflowId);

        workflow.getTasks().clear();
        workflow.setReasonForIncompletion(null);
        workflow.setStartTime(System.currentTimeMillis());
        workflow.setEndTime(0);
        // Change the status to running
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow.setOutput(null);
        workflow.setExternalOutputPayloadStoragePath(null);
        executionDAOFacade.createWorkflow(workflow);
        decide(workflowId);
    }

    /**
     * Gets the last instance of each failed task and reschedule each
     * Gets all cancelled tasks and schedule all of them except JOIN (join should change status to INPROGRESS)
     * Switch workflow back to RUNNING status and call decider.
     *
     * @param workflowId the id of the workflow to be retried
     */
    public void retry(String workflowId) {
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);
        if (!workflow.getStatus().isTerminal()) {
            throw new ApplicationException(CONFLICT, "Workflow is still running.  status=" + workflow.getStatus());
        }
        if (workflow.getTasks().isEmpty()) {
            throw new ApplicationException(CONFLICT, "Workflow has not started yet");
        }

        // Get all FAILED or CANCELED tasks that are not COMPLETED (or reach other terminal states) on further executions.
        // // Eg: for Seq of tasks task1.CANCELED, task1.COMPLETED, task1 shouldn't be retried.
        // Throw an exception if there are no FAILED tasks.
        // Handle JOIN task CANCELED status as special case.
        Map<String, Task> retriableMap = new HashMap<>();
        for (Task task : workflow.getTasks()) {
            switch (task.getStatus()) {
                case FAILED:
                case FAILED_WITH_TERMINAL_ERROR:
                    retriableMap.put(task.getReferenceTaskName(), task);
                    break;
                case CANCELED:
                    if (task.getTaskType().equalsIgnoreCase(TaskType.JOIN.toString())) {
                        task.setStatus(IN_PROGRESS);
                        // Task doesn't have to be updated yet. Will be updated along with other Workflow tasks downstream.
                    } else {
                        retriableMap.put(task.getReferenceTaskName(), task);
                    }
                    break;
                default:
                    retriableMap.remove(task.getReferenceTaskName());
                    break;
            }
        }

        if (retriableMap.values().size() == 0) {
            throw new ApplicationException(CONFLICT,
                    "There are no retriable tasks! Use restart if you want to attempt entire workflow execution again.");
        }

        // Update Workflow with new status.
        // This should load Workflow from archive, if archived.
        workflow.setStatus(WorkflowStatus.RUNNING);
        executionDAOFacade.updateWorkflow(workflow);

        // taskToBeRescheduled would set task `retried` to true, and hence it's important to updateTasks after obtaining task copy from taskToBeRescheduled.
        List<Task> retriableTasks = retriableMap.values().stream()
                .sorted(Comparator.comparingInt(Task::getSeq))
                .map(this::taskToBeRescheduled)
                .collect(Collectors.toList());

        dedupAndAddTasks(workflow, retriableTasks);
        // Note: updateTasks before updateWorkflow might fail when Workflow is archived and doesn't exist in primary store.
        executionDAOFacade.updateTasks(workflow.getTasks());
        scheduleTask(workflow, retriableTasks);

        decide(workflowId);
    }

    /**
     * Reschedule a task
     *
     * @param task failed or cancelled task
     * @return new instance of a task with "SCHEDULED" status
     */
    private Task taskToBeRescheduled(Task task) {
        Task taskToBeRetried = task.copy();
        taskToBeRetried.setTaskId(IDGenerator.generate());
        taskToBeRetried.setRetriedTaskId(task.getTaskId());
        taskToBeRetried.setStatus(SCHEDULED);
        taskToBeRetried.setRetryCount(task.getRetryCount() + 1);
        taskToBeRetried.setRetried(false);
        taskToBeRetried.setPollCount(0);
        taskToBeRetried.setCallbackAfterSeconds(0);
        task.setRetried(true);
        task.setExecuted(true); // since this task is being retried and a retry has been computed, task lifecycle is complete
        return taskToBeRetried;
    }

    public Task getPendingTaskByWorkflow(String taskReferenceName, String workflowId) {
        return executionDAOFacade.getTasksForWorkflow(workflowId).stream()
                .filter(isNonTerminalTask)
                .filter(task -> task.getReferenceTaskName().equals(taskReferenceName))
                .findFirst() // There can only be one task by a given reference name running at a time.
                .orElse(null);
    }

    public List<Task> filterTaskByTypeAndStatusForWorkflow(String workflowId, TaskType taskType, Task.Status taskStatus) {
        return executionDAOFacade.getTasksForWorkflow(workflowId)
                .stream()
                .filter(task -> task.getTaskType().equals(taskType.name()))
                .filter(task -> task.getStatus() == taskStatus)
                .collect(Collectors.toList());
    }

    /**
     * @param wf the workflow to be completed
     * @throws ApplicationException if workflow is not in terminal state
     */
    @VisibleForTesting
    void completeWorkflow(Workflow wf) {
        LOGGER.debug("Completing workflow execution for {}", wf.getWorkflowId());
        Workflow workflow = executionDAOFacade.getWorkflowById(wf.getWorkflowId(), false);

        if (workflow.getStatus().equals(WorkflowStatus.COMPLETED)) {
            queueDAO.remove(DECIDER_QUEUE, workflow.getWorkflowId());    //remove from the sweep queue
            executionDAOFacade.removeFromPendingWorkflow(workflow.getWorkflowName(), workflow.getWorkflowId());
            LOGGER.debug("Workflow: {} has already been completed.", wf.getWorkflowId());
            return;
        }

        if (workflow.getStatus().isTerminal()) {
            String msg = "Workflow is already in terminal state. Current status: " + workflow.getStatus();
            throw new ApplicationException(CONFLICT, msg);
        }

        // FIXME Backwards compatibility for legacy workflows already running.
        // This code will be removed in a future version.
        if (workflow.getWorkflowDefinition() == null) {
            workflow = metadataMapperService.populateWorkflowWithDefinitions(workflow);
        }
        deciderService.updateWorkflowOutput(wf, null);

        workflow.setStatus(WorkflowStatus.COMPLETED);
        workflow.setTasks(wf.getTasks());
        workflow.setOutput(wf.getOutput());
        workflow.setExternalOutputPayloadStoragePath(wf.getExternalOutputPayloadStoragePath());
        executionDAOFacade.updateWorkflow(workflow);
        executionDAOFacade.updateTasks(workflow.getTasks());
        LOGGER.debug("Completed workflow execution for {}", workflow.getWorkflowId());

        // If the following task, for some reason fails, the sweep will take care of this again!
        if (workflow.getParentWorkflowId() != null) {
            Workflow parent = executionDAOFacade.getWorkflowById(workflow.getParentWorkflowId(), false);
            WorkflowDef parentDef = Optional.ofNullable(parent.getWorkflowDefinition())
                    .orElseGet(() -> metadataDAO.get(parent.getWorkflowName(), parent.getWorkflowVersion())
                            .orElseThrow(() -> new ApplicationException(NOT_FOUND, String.format("Unable to find parent workflow definition for %s", wf.getWorkflowId())))
                    );
            LOGGER.debug("Completed sub-workflow {}, deciding parent workflow {}", workflow.getWorkflowId(), workflow.getParentWorkflowId());

            Task parentWorkflowTask = executionDAOFacade.getTaskById(workflow.getParentWorkflowTaskId());
            // If parent is FAILED and the sub workflow task in parent is FAILED, we want to resume them
            if (StringUtils.isBlank(parentDef.getFailureWorkflow()) && parent.getStatus() == WorkflowStatus.FAILED && parentWorkflowTask.getStatus() == FAILED) {
                parentWorkflowTask.setStatus(IN_PROGRESS);
                executionDAOFacade.updateTask(parentWorkflowTask);
                parent.setStatus(WorkflowStatus.RUNNING);
                executionDAOFacade.updateWorkflow(parent);
            }
            decide(parent.getWorkflowId());
        }
        Monitors.recordWorkflowCompletion(workflow.getWorkflowName(), workflow.getEndTime() - workflow.getStartTime(), workflow.getOwnerApp());
        queueDAO.remove(DECIDER_QUEUE, workflow.getWorkflowId());    //remove from the sweep queue

        workflowStatusListener.onWorkflowCompleted(workflow);
        executionDAOFacade.updateWorkflow(workflow);
        executionLockService.releaseLock(workflow.getWorkflowId());
        executionLockService.deleteLock(workflow.getWorkflowId());
    }

    public void terminateWorkflow(String workflowId, String reason) {
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);
        if (WorkflowStatus.COMPLETED.equals(workflow.getStatus())) {
            throw new ApplicationException(CONFLICT, "Cannot terminate a COMPLETED workflow.");
        }
        workflow.setStatus(WorkflowStatus.TERMINATED);
        terminateWorkflow(workflow, reason, null);
    }

    /**
     * @param workflow        the workflow to be terminated
     * @param reason          the reason for termination
     * @param failureWorkflow the failure workflow (if any) to be triggered as a result of this termination
     */
    public void terminateWorkflow(Workflow workflow, String reason, String failureWorkflow) {
        try {
            executionLockService.acquireLock(workflow.getWorkflowId(), 60000);

            if (!workflow.getStatus().isTerminal()) {
                workflow.setStatus(WorkflowStatus.TERMINATED);
            }

            // FIXME Backwards compatibility for legacy workflows already running.
            // This code will be removed in a future version.
            if (workflow.getWorkflowDefinition() == null) {
                workflow = metadataMapperService.populateWorkflowWithDefinitions(workflow);
            }
            deciderService.updateWorkflowOutput(workflow, null);

            String workflowId = workflow.getWorkflowId();
            workflow.setReasonForIncompletion(reason);
            executionDAOFacade.updateWorkflow(workflow);

            List<Task> tasks = workflow.getTasks();
            for (Task task : tasks) {
                if (!task.getStatus().isTerminal()) {
                    // Cancel the ones which are not completed yet....
                    task.setStatus(CANCELED);
                    if (isSystemTask.test(task)) {
                        WorkflowSystemTask stt = WorkflowSystemTask.get(task.getTaskType());
                        try {
                            stt.cancel(workflow, task, this);
                        } catch (Exception e) {
                            throw new ApplicationException(
                                    Code.INTERNAL_ERROR,
                                    String.format("Error canceling systems task: %s", stt.getName()),
                                    e
                            );
                        }
                    }
                    executionDAOFacade.updateTask(task);
                }
                // And remove from the task queue if they were there
                queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId());
            }

            // If the following lines, for some reason fails, the sweep will take
            // care of this again!
            if (workflow.getParentWorkflowId() != null) {
                Workflow parent = executionDAOFacade.getWorkflowById(workflow.getParentWorkflowId(), false);
                decide(parent.getWorkflowId());
            }

            if (!StringUtils.isBlank(failureWorkflow)) {
                Map<String, Object> input = new HashMap<>(workflow.getInput());
                input.put("workflowId", workflowId);
                input.put("reason", reason);
                input.put("failureStatus", workflow.getStatus().toString());

                try {
                    WorkflowDef latestFailureWorkflow = metadataDAO.getLatest(failureWorkflow)
                            .orElseThrow(() ->
                                    new RuntimeException("Failure Workflow Definition not found for: " + failureWorkflow)
                            );

                    String failureWFId = startWorkflow(
                            latestFailureWorkflow,
                            input,
                            null,
                            workflowId,
                            null,
                            null
                    );

                    workflow.getOutput().put("conductor.failure_workflow", failureWFId);
                } catch (Exception e) {
                    LOGGER.error("Failed to start error workflow", e);
                    workflow.getOutput().put("conductor.failure_workflow", "Error workflow " + failureWorkflow + " failed to start.  reason: " + e.getMessage());
                    Monitors.recordWorkflowStartError(failureWorkflow, WorkflowContext.get().getClientApp());
                }
            }

            queueDAO.remove(DECIDER_QUEUE, workflow.getWorkflowId());    //remove from the sweep queue
            executionDAOFacade.removeFromPendingWorkflow(workflow.getWorkflowName(), workflow.getWorkflowId());

            // Send to atlas
            Monitors.recordWorkflowTermination(workflow.getWorkflowName(), workflow.getStatus(), workflow.getOwnerApp());

            workflowStatusListener.onWorkflowTerminated(workflow);
            executionDAOFacade.updateWorkflow(workflow);

        } finally {
            executionLockService.releaseLock(workflow.getWorkflowId());
            executionLockService.deleteLock(workflow.getWorkflowId());
        }
    }

    /**
     * @param taskResult the task result to be updated
     * @throws ApplicationException
     */
    public void updateTask(TaskResult taskResult) {
        if (taskResult == null) {
            throw new ApplicationException(Code.INVALID_INPUT, "Task object is null");
        }

        String workflowId = taskResult.getWorkflowInstanceId();
        Workflow workflowInstance = executionDAOFacade.getWorkflowById(workflowId, true);

        // FIXME Backwards compatibility for legacy workflows already running.
        // This code will be removed in a future version.
        if (workflowInstance.getWorkflowDefinition() == null) {
            workflowInstance = metadataMapperService.populateWorkflowWithDefinitions(workflowInstance);
        }

        Task task = Optional.ofNullable(executionDAOFacade.getTaskById(taskResult.getTaskId()))
                .orElseThrow(() -> new ApplicationException(Code.NOT_FOUND, "No such task found by id: " + taskResult.getTaskId()));

        LOGGER.debug("Task: {} belonging to Workflow {} being updated", task, workflowInstance);

        String taskQueueName = QueueUtils.getQueueName(task);

        if (task.getStatus().isTerminal()) {
            // Task was already updated....
            queueDAO.remove(taskQueueName, taskResult.getTaskId());
            LOGGER.info("Task: {} has already finished execution with status: {} within workflow: {}. Removed task from queue: {}", task.getTaskId(), task.getStatus(), task.getWorkflowInstanceId(), taskQueueName);
            Monitors.recordUpdateConflict(task.getTaskType(), workflowInstance.getWorkflowName(), task.getStatus());
            return;
        }

        if (workflowInstance.getStatus().isTerminal()) {
            // Workflow is in terminal state
            queueDAO.remove(taskQueueName, taskResult.getTaskId());
            LOGGER.info("Workflow: {} has already finished execution. Task update for: {} ignored and removed from Queue: {}.", workflowInstance, taskResult.getTaskId(), taskQueueName);
            Monitors.recordUpdateConflict(task.getTaskType(), workflowInstance.getWorkflowName(), workflowInstance.getStatus());
            return;
        }

        // for system tasks, setting to SCHEDULED would mean restarting the task which is undesirable
        // for worker tasks, set status to SCHEDULED and push to the queue
        if (!isSystemTask.test(task) && taskResult.getStatus() == Status.IN_PROGRESS) {
            task.setStatus(SCHEDULED);
        } else {
            task.setStatus(valueOf(taskResult.getStatus().name()));
        }
        task.setOutputMessage(taskResult.getOutputMessage());
        task.setReasonForIncompletion(taskResult.getReasonForIncompletion());
        task.setWorkerId(taskResult.getWorkerId());
        task.setCallbackAfterSeconds(taskResult.getCallbackAfterSeconds());
        task.setOutputData(taskResult.getOutputData());

        if (task.getOutputData() != null) {
            deciderService.externalizeTaskData(task);
        } else {
            task.setExternalOutputPayloadStoragePath(taskResult.getExternalOutputPayloadStoragePath());
        }

        if (task.getStatus().isTerminal()) {
            task.setEndTime(System.currentTimeMillis());
        }

        // Fails the workflow if any of the below operations fail.
        // This helps avoid workflow inconsistencies. For example, for the taskResult with status:COMPLETED,
        // if update task to primary data store is successful, but remove from queue fails,
        // The decide wouldn't run and next task will not be scheduled.
        // TODO Try to recover the workflow.
        try {
            String updateTaskQueueDesc = "Updating Task queues for taskId: " + task.getTaskId();
            String taskQueueOperation = "updateTaskQueues";
            String updateTaskDesc = "Updating Task with taskId: " + task.getTaskId();
            String updateTaskOperation = "updateTask";

            // Retry each operation twice before failing workflow.
            new RetryUtil<>().retryOnException(() -> {
                switch (task.getStatus()) {
                    case COMPLETED:
                    case CANCELED:
                    case FAILED:
                    case FAILED_WITH_TERMINAL_ERROR:
                    case TIMED_OUT:
                        queueDAO.remove(taskQueueName, taskResult.getTaskId());
                        LOGGER.debug("Task: {} removed from taskQueue: {} since the task status is {}", task, taskQueueName, task.getStatus().name());
                        break;
                    case IN_PROGRESS:
                    case SCHEDULED:
                        // put it back in queue based on callbackAfterSeconds
                        long callBack = taskResult.getCallbackAfterSeconds();
                        queueDAO.remove(taskQueueName, task.getTaskId());
                        LOGGER.debug("Task: {} removed from taskQueue: {} since the task status is {}", task, taskQueueName, task.getStatus().name());
                        queueDAO.push(taskQueueName, task.getTaskId(), task.getWorkflowPriority(), callBack); // Milliseconds
                        LOGGER.debug("Task: {} pushed back to taskQueue: {} since the task status is {} with callbackAfterSeconds: {}", task, taskQueueName, task.getStatus().name(), callBack);
                        break;
                    default:
                        break;
                };
                return null;
            }, null, null, 2, updateTaskQueueDesc, taskQueueOperation);

            new RetryUtil<>().retryOnException(() -> {
                executionDAOFacade.updateTask(task);
                return null;
            }, null, null, 2, updateTaskDesc, updateTaskOperation);

            //If the task has failed update the failed task reference name in the workflow.
            //This gives the ability to look at workflow and see what tasks have failed at a high level.
            if (FAILED.equals(task.getStatus()) || FAILED_WITH_TERMINAL_ERROR.equals(task.getStatus())) {
                workflowInstance.getFailedReferenceTaskNames().add(task.getReferenceTaskName());
                executionDAOFacade.updateWorkflow(workflowInstance);
                LOGGER.debug("Task: {} has a {} status and the Workflow has been updated with failed task reference", task, task.getStatus());
            }
        } catch (Exception e) {
            String errorMsg = String.format("Error updating task: %s for workflow: %s", task.getTaskId(), workflowId);
            LOGGER.error(errorMsg, e);
            Monitors.recordTaskUpdateError(task.getTaskType(), workflowInstance.getWorkflowName());
            throw new ApplicationException(Code.BACKEND_ERROR, e);
        }

        taskResult.getLogs().forEach(taskExecLog -> taskExecLog.setTaskId(task.getTaskId()));
        executionDAOFacade.addTaskExecLog(taskResult.getLogs());

        decide(workflowId);

        if (task.getStatus().isTerminal()) {
            long duration = getTaskDuration(0, task);
            long lastDuration = task.getEndTime() - task.getStartTime();
            Monitors.recordTaskExecutionTime(task.getTaskDefName(), duration, true, task.getStatus());
            Monitors.recordTaskExecutionTime(task.getTaskDefName(), lastDuration, false, task.getStatus());
        }
    }

    public Task getTask(String taskId) {
        return Optional.ofNullable(executionDAOFacade.getTaskById(taskId))
                .map(task -> {
                    if (task.getWorkflowTask() != null) {
                        return metadataMapperService.populateTaskWithDefinition(task);
                    }
                    return task;
                })
                .orElse(null);
    }

    public List<Task> getTasks(String taskType, String startKey, int count) {
        return executionDAOFacade.getTasksByName(taskType, startKey, count);
    }

    public List<Workflow> getRunningWorkflows(String workflowName, int version) {
        return executionDAOFacade.getPendingWorkflowsByName(workflowName, version);

    }

    public List<String> getWorkflows(String name, Integer version, Long startTime, Long endTime) {
        List<Workflow> workflowsByType = executionDAOFacade.getWorkflowsByName(name, startTime, endTime);
        return workflowsByType.stream()
                .filter(workflow -> workflow.getWorkflowVersion() == version)
                .map(Workflow::getWorkflowId)
                .collect(Collectors.toList());

    }

    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        return executionDAOFacade.getRunningWorkflowIds(workflowName, version);
    }

    /**
     * @param workflowId ID of the workflow to evaluate the state for
     * @return true if the workflow has completed (success or failed), false otherwise.
     * @throws ApplicationException If there was an error - caller should retry in this case.
     */
    public boolean decide(String workflowId) {
        if (!executionLockService.acquireLock(workflowId)) {
            return false;
        }

        // If it is a new workflow, the tasks will be still empty even though include tasks is true
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);

        // FIXME Backwards compatibility for legacy workflows already running.
        // This code will be removed in a future version.
        workflow = metadataMapperService.populateWorkflowWithDefinitions(workflow);

        try {
            DeciderService.DeciderOutcome outcome = deciderService.decide(workflow);
            if (outcome.isComplete) {
                completeWorkflow(workflow);
                return true;
            }

            List<Task> tasksToBeScheduled = outcome.tasksToBeScheduled;
            setTaskDomains(tasksToBeScheduled, workflow);
            List<Task> tasksToBeUpdated = outcome.tasksToBeUpdated;
            List<Task> tasksToBeRequeued = outcome.tasksToBeRequeued;
            boolean stateChanged = false;

            if (!tasksToBeRequeued.isEmpty()) {
                addTaskToQueue(tasksToBeRequeued);
            }

            tasksToBeScheduled = dedupAndAddTasks(workflow, tasksToBeScheduled);

            for (Task task : outcome.tasksToBeScheduled) {
                if (isSystemTask.and(isNonTerminalTask).test(task)) {
                    WorkflowSystemTask workflowSystemTask = WorkflowSystemTask.get(task.getTaskType());
                    Workflow workflowInstance = deciderService.populateWorkflowAndTaskData(workflow);
                    try {
                        if (!workflowSystemTask.isAsync() && workflowSystemTask.execute(workflowInstance, task, this)) {
                            // FIXME: temporary hack to workaround TERMINATE task
                            if (TERMINATE.name().equals(task.getTaskType())) {
                                workflow.setStatus(workflowInstance.getStatus());
                                workflow.setOutput(workflowInstance.getOutput());
                                deciderService.externalizeWorkflowData(workflow);
                            }
                            deciderService.externalizeTaskData(task);
                            tasksToBeUpdated.add(task);
                            stateChanged = true;
                        }
                    } catch (Exception e) {
                        throw new ApplicationException(Code.INTERNAL_ERROR, String.format("Unable to start system task: %s", workflowSystemTask.getName()), e);
                    }
                }
            }

            if (!outcome.tasksToBeUpdated.isEmpty()) {
                for (Task task : tasksToBeUpdated) {
                    if (task.getStatus() != null && (!task.getStatus().equals(Task.Status.IN_PROGRESS)
                            || !task.getStatus().equals(Task.Status.SCHEDULED))) {
                        queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId());
                    }
                }
            }

            if (!outcome.tasksToBeUpdated.isEmpty() || !tasksToBeScheduled.isEmpty()) {
                executionDAOFacade.updateTasks(tasksToBeUpdated);
                executionDAOFacade.updateWorkflow(workflow);
                queueDAO.push(DECIDER_QUEUE, workflow.getWorkflowId(), workflow.getPriority(), config.getSweepFrequency());
            }

            stateChanged = scheduleTask(workflow, tasksToBeScheduled) || stateChanged;

            if (stateChanged) {
                decide(workflowId);
            }

        } catch (TerminateWorkflowException twe) {
            LOGGER.info("Execution terminated of workflow: {}", workflowId, twe);
            terminate(workflow, twe);
            return true;
        } catch (RuntimeException e) {
            LOGGER.error("Error deciding workflow: {}", workflowId, e);
            throw e;
        } finally {
            executionLockService.releaseLock(workflowId);
        }
        return false;
    }

    @VisibleForTesting
    List<Task> dedupAndAddTasks(Workflow workflow, List<Task> tasks) {
        List<String> tasksInWorkflow = workflow.getTasks().stream()
                .map(task -> task.getReferenceTaskName() + "_" + task.getRetryCount())
                .collect(Collectors.toList());

        List<Task> dedupedTasks = tasks.stream()
                .filter(task -> !tasksInWorkflow.contains(task.getReferenceTaskName() + "_" + task.getRetryCount()))
                .collect(Collectors.toList());

        workflow.getTasks().addAll(dedupedTasks);
        return dedupedTasks;
    }

    /**
     * @throws ApplicationException if the workflow cannot be paused
     */
    public void pauseWorkflow(String workflowId) {
        try {
            executionLockService.acquireLock(workflowId, 60000);
            WorkflowStatus status = WorkflowStatus.PAUSED;
            Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, false);
            if (workflow.getStatus().isTerminal()) {
                throw new ApplicationException(CONFLICT, "Workflow id " + workflowId + " has ended, status cannot be updated.");
            }
            if (workflow.getStatus().equals(status)) {
                return;        //Already paused!
            }
            workflow.setStatus(status);
            executionDAOFacade.updateWorkflow(workflow);
        } finally {
            executionLockService.releaseLock(workflowId);
        }
    }

    /**
     * @param workflowId
     * @throws IllegalStateException
     */
    public void resumeWorkflow(String workflowId) {
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, false);
        if (!workflow.getStatus().equals(WorkflowStatus.PAUSED)) {
            throw new IllegalStateException("The workflow " + workflowId + " is not PAUSED so cannot resume. " +
                    "Current status is " + workflow.getStatus().name());
        }
        workflow.setStatus(WorkflowStatus.RUNNING);
        executionDAOFacade.updateWorkflow(workflow);
        decide(workflowId);
    }

    /**
     * @param workflowId
     * @param taskReferenceName
     * @param skipTaskRequest
     * @throws IllegalStateException
     */
    public void skipTaskFromWorkflow(String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest) {

        Workflow wf = executionDAOFacade.getWorkflowById(workflowId, true);

        // FIXME Backwards compatibility for legacy workflows already running.
        // This code will be removed in a future version.
        wf = metadataMapperService.populateWorkflowWithDefinitions(wf);

        // If the wf is not running then cannot skip any task
        if (!wf.getStatus().equals(WorkflowStatus.RUNNING)) {
            String errorMsg = String.format("The workflow %s is not running so the task referenced by %s cannot be skipped", workflowId, taskReferenceName);
            throw new IllegalStateException(errorMsg);
        }
        // Check if the reference name is as per the workflowdef
        WorkflowTask wft = wf.getWorkflowDefinition().getTaskByRefName(taskReferenceName);
        if (wft == null) {
            String errorMsg = String.format("The task referenced by %s does not exist in the WorkflowDefinition %s", taskReferenceName, wf.getWorkflowName());
            throw new IllegalStateException(errorMsg);
        }
        // If the task is already started the again it cannot be skipped
        wf.getTasks().forEach(task -> {
            if (task.getReferenceTaskName().equals(taskReferenceName)) {
                String errorMsg = String.format("The task referenced %s has already been processed, cannot be skipped", taskReferenceName);
                throw new IllegalStateException(errorMsg);
            }
        });
        // Now create a "SKIPPED" task for this workflow
        Task theTask = new Task();
        theTask.setTaskId(IDGenerator.generate());
        theTask.setReferenceTaskName(taskReferenceName);
        theTask.setWorkflowInstanceId(workflowId);
        theTask.setWorkflowPriority(wf.getPriority());
        theTask.setStatus(SKIPPED);
        theTask.setTaskType(wft.getName());
        theTask.setCorrelationId(wf.getCorrelationId());
        if (skipTaskRequest != null) {
            theTask.setInputData(skipTaskRequest.getTaskInput());
            theTask.setOutputData(skipTaskRequest.getTaskOutput());
            theTask.setInputMessage(skipTaskRequest.getTaskInputMessage());
            theTask.setOutputMessage(skipTaskRequest.getTaskOutputMessage());
        }
        executionDAOFacade.createTasks(Collections.singletonList(theTask));
        decide(workflowId);
    }

    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        return executionDAOFacade.getWorkflowById(workflowId, includeTasks);
    }

    public void addTaskToQueue(Task task) {
        // put in queue
        String taskQueueName = QueueUtils.getQueueName(task);
        queueDAO.remove(taskQueueName, task.getTaskId());
        if (task.getCallbackAfterSeconds() > 0) {
            queueDAO.push(taskQueueName, task.getTaskId(), task.getWorkflowPriority(), task.getCallbackAfterSeconds());
        } else {
            queueDAO.push(taskQueueName, task.getTaskId(), task.getWorkflowPriority(), 0);
        }
        LOGGER.debug("Added task {} with priority {} to queue {} with call back seconds {}", task, task.getWorkflowPriority(), taskQueueName, task.getCallbackAfterSeconds());
    }

    //Executes the async system task
    public void executeSystemTask(WorkflowSystemTask systemTask, String taskId, int unackTimeout) {
        try {
            Task task = executionDAOFacade.getTaskById(taskId);
            if (task == null) {
                LOGGER.error("TaskId: {} could not be found while executing SystemTask", taskId);
                return;
            }
            LOGGER.debug("Task: {} fetched from execution DAO for taskId: {}", task, taskId);
            if (task.getStatus().isTerminal()) {
                //Tune the SystemTaskWorkerCoordinator's queues - if the queue size is very big this can happen!
                LOGGER.info("Task {}/{} was already completed.", task.getTaskType(), task.getTaskId());
                queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId());
                return;
            }

            String workflowId = task.getWorkflowInstanceId();
            Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);

            if (task.getStartTime() == 0) {
                task.setStartTime(System.currentTimeMillis());
                Monitors.recordQueueWaitTime(task.getTaskDefName(), task.getQueueWaitTime());
            }

            if (workflow.getStatus().isTerminal()) {
                LOGGER.info("Workflow {} has been completed for {}/{}", workflow.getWorkflowId(), systemTask.getName(), task.getTaskId());
                if (!task.getStatus().isTerminal()) {
                    task.setStatus(CANCELED);
                }
                executionDAOFacade.updateTask(task);
                queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId());
                return;
            }

            if (task.getStatus().equals(SCHEDULED)) {
                if (executionDAOFacade.exceedsInProgressLimit(task)) {
                    //to do add a metric to record this
                    LOGGER.warn("Concurrent Execution limited for {}:{}", taskId, task.getTaskDefName());
                    return;
                }
                if (task.getRateLimitPerFrequency() > 0 && executionDAOFacade.exceedsRateLimitPerFrequency(task, metadataDAO.getTaskDef(task.getTaskDefName()))) {
                    LOGGER.warn("RateLimit Execution limited for {}:{}, limit:{}", taskId, task.getTaskDefName(), task.getRateLimitPerFrequency());
                    return;
                }
            }

            LOGGER.debug("Executing {}/{}-{}", task.getTaskType(), task.getTaskId(), task.getStatus());
            queueDAO.setUnackTimeout(QueueUtils.getQueueName(task), task.getTaskId(), systemTask.getRetryTimeInSecond() * 1000);
            if (task.getStatus() == SCHEDULED || !systemTask.isAsyncComplete(task)) {
                task.setPollCount(task.getPollCount() + 1);
                executionDAOFacade.updateTask(task);
            }

            deciderService.populateTaskData(task);

            // Stop polling for asyncComplete system tasks that are not in SCHEDULED state
            if (systemTask.isAsyncComplete(task) && task.getStatus() != SCHEDULED) {
                queueDAO.ack(QueueUtils.getQueueName(task), task.getTaskId());
                return;
            }

            switch (task.getStatus()) {
                case SCHEDULED:
                    systemTask.start(workflow, task, this);
                    break;

                case IN_PROGRESS:
                    systemTask.execute(workflow, task, this);
                    break;
                default:
                    break;
            }

            if (!task.getStatus().isTerminal()) {
                task.setCallbackAfterSeconds(unackTimeout);
            }

            updateTask(new TaskResult(task));
            LOGGER.debug("Done Executing {}/{}-{} output={}", task.getTaskType(), task.getTaskId(), task.getStatus(),
                task.getOutputData().toString());

        } catch (Exception e) {
            LOGGER.error("Error executing system task - {}, with id: {}", systemTask, taskId, e);
        }
    }

    @VisibleForTesting
    void setTaskDomains(List<Task> tasks, Workflow wf) {
        Map<String, String> taskToDomain = wf.getTaskToDomain();
        if (taskToDomain != null) {
            // Step 1: Apply * mapping to all tasks, if present.
            String domainstr = taskToDomain.get("*");
            if (domainstr != null) {
                String[] domains = domainstr.split(",");
                tasks.forEach(task -> {
                    // Filter out SystemTask
                    if (!TaskType.isSystemTask(task.getTaskType())) {
                        // Check which domain worker is polling
                        // Set the task domain
                        task.setDomain(getActiveDomain(task.getTaskType(), domains));
                    }
                });

            }
            // Step 2: Override additional mappings.
            tasks.forEach(task -> {
                if (!TaskType.isSystemTask(task.getTaskType())) {
                    String taskDomainstr = taskToDomain.get(task.getTaskType());
                    if (taskDomainstr != null) {
                        task.setDomain(getActiveDomain(task.getTaskType(), taskDomainstr.split(",")));
                    }
                }
            });
        }
    }

    /**
     * Gets the active domain from the list of domains where the task is to be queued.
     * The domain list must be ordered.
     * In sequence, check if any worker has polled for last `activeWorkerLastPollInSecs` seconds, if so that is the Active domain.
     * When no active domains are found:
     *   <li> If NO_DOMAIN token is provided, return null.
     *   <li> Else, return last domain from list.
     *
     * @param taskType the taskType of the task for which active domain is to be found
     * @param domains  the array of domains for the task. (Must contain atleast one element).
     * @return the active domain where the task will be queued
     */
    @VisibleForTesting
    String getActiveDomain(String taskType, String[] domains) {
        if (domains == null || domains.length == 0) {
            return null;
        }

        return Arrays.stream(domains)
                .filter(domain -> !domain.equalsIgnoreCase("NO_DOMAIN"))
                .map(domain -> executionDAOFacade.getTaskPollDataByDomain(taskType, domain.trim()))
                .filter(Objects::nonNull)
                .filter(validateLastPolledTime)
                .findFirst()
                .map(PollData::getDomain)
                .orElse(domains[domains.length - 1].trim().equalsIgnoreCase("NO_DOMAIN") ? null : domains[domains.length - 1].trim());
    }

    private long getTaskDuration(long s, Task task) {
        long duration = task.getEndTime() - task.getStartTime();
        s += duration;
        if (task.getRetriedTaskId() == null) {
            return s;
        }
        return s + getTaskDuration(s, executionDAOFacade.getTaskById(task.getRetriedTaskId()));
    }

    @VisibleForTesting
    boolean scheduleTask(Workflow workflow, List<Task> tasks) {
        List<Task> createdTasks;

        try {
            if (tasks == null || tasks.isEmpty()) {
                return false;
            }

            // Get the highest seq number
            int count = workflow.getTasks().stream()
                    .mapToInt(Task::getSeq)
                    .max()
                    .orElse(0);

            for (Task task : tasks) {
                if (task.getSeq() == 0) { // Set only if the seq was not set
                    task.setSeq(++count);
                }
            }

            // Save the tasks in the DAO
            createdTasks = executionDAOFacade.createTasks(tasks);

            List<Task> systemTasks = createdTasks.stream()
                    .filter(isSystemTask)
                    .collect(Collectors.toList());

            List<Task> tasksToBeQueued = createdTasks.stream()
                    .filter(isSystemTask.negate())
                    .collect(Collectors.toList());

            boolean startedSystemTasks = false;

            // Traverse through all the system tasks, start the sync tasks, in case of async queue the tasks
            for (Task task : systemTasks) {
                WorkflowSystemTask workflowSystemTask = WorkflowSystemTask.get(task.getTaskType());
                if (workflowSystemTask == null) {
                    throw new ApplicationException(NOT_FOUND, "No system task found by name " + task.getTaskType());
                }
                if (task.getStatus() != null && !task.getStatus().isTerminal() && task.getStartTime() == 0) {
                    task.setStartTime(System.currentTimeMillis());
                }
                if (!workflowSystemTask.isAsync()) {
                    try {
                        deciderService.populateTaskData(task);
                        workflowSystemTask.start(workflow, task, this);
                    } catch (Exception e) {
                        String errorMsg = String.format("Unable to start system task: %s, {id: %s, name: %s}", task.getTaskType(), task.getTaskId(), task.getTaskDefName());
                        throw new ApplicationException(Code.INTERNAL_ERROR, errorMsg, e);
                    }
                    startedSystemTasks = true;
                    deciderService.externalizeTaskData(task);
                    executionDAOFacade.updateTask(task);
                } else {
                    tasksToBeQueued.add(task);
                }
            }

            addTaskToQueue(tasksToBeQueued);
            return startedSystemTasks;
        } catch (Exception e) {
            List<String> taskIds = tasks.stream()
                    .map(Task::getTaskId)
                    .collect(Collectors.toList());
            String errorMsg = String.format("Error scheduling tasks: %s, for workflow: %s", taskIds, workflow.getWorkflowId());
            LOGGER.error(errorMsg, e);
            Monitors.error(className, "scheduleTask");
            // TODO Provide a better implementation of rollbackTasks considering all the edge cases.
            // Throwing exception to avoid workflow ending up in irrecoverable state.
            // rollbackTasks(workflow.getWorkflowId(), createdTasks);
            throw new TerminateWorkflowException(errorMsg);
        }
    }

    /**
     * Rolls back all newly created tasks in a workflow, essentially resetting the workflow state, in case of an exception during task creation or task enqueuing.
     *
     * @param createdTasks a {@link List} of newly created tasks in the workflow which are to be rolled back
     */
    @VisibleForTesting
    void rollbackTasks(String workflowId, List<Task> createdTasks) {
        String description = "rolling back task from DAO for " + workflowId;
        String operation = "rollbackTasks";

        try {
            // rollback all newly created tasks in the workflow
            createdTasks.forEach(task -> new RetryUtil<>().retryOnException(() ->
            {
                if (task.getTaskType().equals(SUB_WORKFLOW.name())) {
                    executionDAOFacade.removeWorkflow((String) task.getOutputData().get(SUB_WORKFLOW_ID), false);
                }
                executionDAOFacade.removeTask(task.getTaskId());
                return null;
            }, null, null, 3, description, operation));
        } catch (Exception e) {
            String errorMsg = String.format("Error scheduling/rolling back tasks for workflow: %s", workflowId);
            LOGGER.error(errorMsg, e);
            throw new TerminateWorkflowException(errorMsg);
        }
    }

    private void addTaskToQueue(final List<Task> tasks) {
        for (Task task : tasks) {
            addTaskToQueue(task);
        }
    }

    private void terminate(final Workflow workflow, TerminateWorkflowException tw) {
        if (!workflow.getStatus().isTerminal()) {
            workflow.setStatus(tw.workflowStatus);
        }

        String failureWorkflow = workflow.getWorkflowDefinition().getFailureWorkflow();
        if (failureWorkflow != null) {
            if (failureWorkflow.startsWith("$")) {
                String[] paramPathComponents = failureWorkflow.split("\\.");
                String name = paramPathComponents[2]; // name of the input parameter
                failureWorkflow = (String) workflow.getInput().get(name);
            }
        }
        if (tw.task != null) {
            executionDAOFacade.updateTask(tw.task);
        }
        terminateWorkflow(workflow, tw.getMessage(), failureWorkflow);
    }

    private boolean rerunWF(String workflowId, String taskId, Map<String, Object> taskInput,
                            Map<String, Object> workflowInput, String correlationId) {

        // Get the workflow
        Workflow workflow = executionDAOFacade.getWorkflowById(workflowId, true);

        // If the task Id is null it implies that the entire workflow has to be rerun
        if (taskId == null) {
            // remove all tasks
            workflow.getTasks().forEach(task -> executionDAOFacade.removeTask(task.getTaskId()));
            // Set workflow as RUNNING
            workflow.setStatus(WorkflowStatus.RUNNING);
            if (correlationId != null) {
                workflow.setCorrelationId(correlationId);
            }
            if (workflowInput != null) {
                workflow.setInput(workflowInput);
            }

            executionDAOFacade.updateWorkflow(workflow);

            decide(workflowId);
            return true;
        }

        // Now iterate through the tasks and find the "specific" task
        Task rerunFromTask = null;
        for (Task task : workflow.getTasks()) {
            if (task.getTaskId().equals(taskId)) {
                rerunFromTask = task;
                break;
            } else {
                // If not found look into sub workflows
                if (task.getTaskType().equalsIgnoreCase(SubWorkflow.NAME)) {
                    String subWorkflowId = task.getInputData().get(SUB_WORKFLOW_ID).toString();
                    if (rerunWF(subWorkflowId, taskId, taskInput, null, null)) {
                        rerunFromTask = task;
                        break;
                    }
                }
            }
        }

        if (rerunFromTask != null) {
            // set workflow as RUNNING
            workflow.setStatus(WorkflowStatus.RUNNING);
            if (correlationId != null) {
                workflow.setCorrelationId(correlationId);
            }
            if (workflowInput != null) {
                workflow.setInput(workflowInput);
            }
            executionDAOFacade.updateWorkflow(workflow);
            //update tasks in datastore to update workflow-tasks relationship for archived workflows
            executionDAOFacade.updateTasks(workflow.getTasks());
            // Remove all tasks after the "rerunFromTask"
            for (Task task : workflow.getTasks()) {
                if (task.getSeq() > rerunFromTask.getSeq()) {
                    executionDAOFacade.removeTask(task.getTaskId());
                }
            }
            //reset fields before restarting the task
            rerunFromTask.setScheduledTime(System.currentTimeMillis());
            rerunFromTask.setStartTime(0);
            rerunFromTask.setUpdateTime(0);
            rerunFromTask.setEndTime(0);
            rerunFromTask.setOutputData(null);
            rerunFromTask.setExternalOutputPayloadStoragePath(null);
            if (rerunFromTask.getTaskType().equalsIgnoreCase(SubWorkflow.NAME)) {
                // if task is sub workflow set task as IN_PROGRESS and reset start time
                rerunFromTask.setStatus(IN_PROGRESS);
                rerunFromTask.setStartTime(System.currentTimeMillis());
            } else {
                // Set the task to rerun as SCHEDULED
                rerunFromTask.setStatus(SCHEDULED);
                if (taskInput != null) {
                    rerunFromTask.setInputData(taskInput);
                }
                addTaskToQueue(rerunFromTask);
            }
            rerunFromTask.setExecuted(false);
            executionDAOFacade.updateTask(rerunFromTask);

            decide(workflowId);
            return true;
        }
        return false;
    }

    public void scheduleNextIteration(Task loopTask, Workflow workflow) {
        //Schedule only first loop over task. Rest will be taken care in Decider Service when this task will get completed.
        List<Task> scheduledLoopOverTasks = deciderService.getTasksToBeScheduled(workflow, loopTask.getWorkflowTask().getLoopOver().get(0), loopTask.getRetryCount(), null);
        scheduledLoopOverTasks.stream().forEach(t -> {
            t.setReferenceTaskName(TaskUtils.appendIteration(t.getReferenceTaskName(), loopTask.getIteration()));
            t.setIteration(loopTask.getIteration());
        });
        scheduleTask(workflow, scheduledLoopOverTasks);
    }

    public TaskDef getTaskDefinition(Task task) {
        return task.getTaskDefinition()
                .orElseGet(() -> Optional.ofNullable(metadataDAO.getTaskDef(task.getWorkflowTask().getName()))
                        .orElseThrow(() -> {
                            String reason = String.format("Invalid task specified. Cannot find task by name %s in the task definitions", task.getWorkflowTask().getName());
                            return new TerminateWorkflowException(reason);
                        }));
    }
}

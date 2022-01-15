/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.core.dal;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.Operation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.PayloadType;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.domain.TaskDO;
import com.netflix.conductor.domain.TaskStatusDO;
import com.netflix.conductor.domain.WorkflowDO;
import com.netflix.conductor.domain.WorkflowStatusDO;
import com.netflix.conductor.metrics.Monitors;

@Component
public class DomainMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DomainMapper.class);

    private final ExternalPayloadStorageUtils externalPayloadStorageUtils;

    public DomainMapper(ExternalPayloadStorageUtils externalPayloadStorageUtils) {
        this.externalPayloadStorageUtils = externalPayloadStorageUtils;
    }

    /**
     * Fetch the fully formed workflow domain object with complete payloads
     *
     * @param workflowDO the workflow domain object from the datastore
     * @return the workflow domain object {@link WorkflowDO} with payloads from external storage
     */
    public WorkflowDO getWorkflowDO(WorkflowDO workflowDO) {
        populateWorkflowAndTaskPayloadData(workflowDO);
        return workflowDO;
    }

    public WorkflowDO getLeanWorkflowDO(WorkflowDO workflowDO) {
        externalizeWorkflowData(workflowDO);
        return workflowDO;
    }

    public Workflow getWorkflowDTO(WorkflowDO workflowDO) {
        externalizeWorkflowData(workflowDO);

        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatusDO.getWorkflowStatusDTO(workflowDO.getStatus()));
        workflow.setEndTime(workflowDO.getEndTime());
        workflow.setWorkflowId(workflowDO.getWorkflowId());
        workflow.setParentWorkflowId(workflowDO.getParentWorkflowId());
        workflow.setParentWorkflowTaskId(workflowDO.getParentWorkflowTaskId());
        workflow.setInput(workflowDO.getInput());
        workflow.setOutput(workflowDO.getOutput());
        workflow.setCorrelationId(workflow.getCorrelationId());
        workflow.setReRunFromWorkflowId(workflowDO.getReRunFromWorkflowId());
        workflow.setReasonForIncompletion(workflowDO.getReasonForIncompletion());
        workflow.setEvent(workflowDO.getEvent());
        workflow.setTaskToDomain(workflowDO.getTaskToDomain());
        workflow.setFailedReferenceTaskNames(workflowDO.getFailedReferenceTaskNames());
        workflow.setWorkflowDefinition(workflowDO.getWorkflowDefinition());
        workflow.setExternalInputPayloadStoragePath(
                workflowDO.getExternalInputPayloadStoragePath());
        workflow.setExternalOutputPayloadStoragePath(
                workflowDO.getExternalOutputPayloadStoragePath());
        workflow.setPriority(workflowDO.getPriority());
        workflow.setVariables(workflowDO.getVariables());
        workflow.setLastRetriedTime(workflowDO.getLastRetriedTime());
        workflow.setOwnerApp(workflowDO.getOwnerApp());
        workflow.setCreateTime(workflowDO.getCreatedTime());
        workflow.setUpdateTime(workflowDO.getUpdatedTime());
        workflow.setCreatedBy(workflowDO.getCreatedBy());
        workflow.setUpdatedBy(workflowDO.getUpdatedBy());

        workflow.setTasks(
                workflowDO.getTasks().stream().map(this::getTaskDTO).collect(Collectors.toList()));

        return workflow;
    }

    public TaskDO getTaskDO(TaskDO taskDO) {
        populateTaskData(taskDO);
        return taskDO;
    }

    public TaskDO getLeanTaskDO(TaskDO taskDO) {
        externalizeTaskData(taskDO);
        return taskDO;
    }

    public Task getTaskDTO(TaskDO taskDO) {
        externalizeTaskData(taskDO);

        Task task = new Task();
        task.setTaskType(taskDO.getTaskType());
        task.setStatus(TaskStatusDO.getTaskStatusDTO(taskDO.getStatus()));
        task.setInputData(taskDO.getInputData());
        task.setReferenceTaskName(taskDO.getReferenceTaskName());
        task.setRetryCount(taskDO.getRetryCount());
        task.setSeq(taskDO.getSeq());
        task.setCorrelationId(taskDO.getCorrelationId());
        task.setPollCount(taskDO.getPollCount());
        task.setTaskDefName(taskDO.getTaskDefName());
        task.setScheduledTime(taskDO.getScheduledTime());
        task.setStartTime(taskDO.getStartTime());
        task.setEndTime(taskDO.getEndTime());
        task.setUpdateTime(taskDO.getUpdateTime());
        task.setStartDelayInSeconds(taskDO.getStartDelayInSeconds());
        task.setRetriedTaskId(taskDO.getRetriedTaskId());
        task.setRetried(taskDO.isRetried());
        task.setExecuted(taskDO.isExecuted());
        task.setCallbackFromWorker(taskDO.isCallbackFromWorker());
        task.setResponseTimeoutSeconds(taskDO.getResponseTimeoutSeconds());
        task.setWorkflowInstanceId(taskDO.getWorkflowInstanceId());
        task.setWorkflowType(taskDO.getWorkflowType());
        task.setTaskId(taskDO.getTaskId());
        task.setReasonForIncompletion(taskDO.getReasonForIncompletion());
        task.setCallbackAfterSeconds(taskDO.getCallbackAfterSeconds());
        task.setWorkerId(taskDO.getWorkerId());
        task.setOutputData(taskDO.getOutputData());
        task.setWorkflowTask(taskDO.getWorkflowTask());
        task.setDomain(taskDO.getDomain());
        task.setInputMessage(taskDO.getInputMessage());
        task.setOutputMessage(taskDO.getOutputMessage());
        task.setRateLimitPerFrequency(taskDO.getRateLimitPerFrequency());
        task.setRateLimitFrequencyInSeconds(taskDO.getRateLimitFrequencyInSeconds());
        task.setExternalInputPayloadStoragePath(taskDO.getExternalInputPayloadStoragePath());
        task.setExternalOutputPayloadStoragePath(taskDO.getExternalOutputPayloadStoragePath());
        task.setWorkflowPriority(taskDO.getWorkflowPriority());
        task.setExecutionNameSpace(taskDO.getExecutionNameSpace());
        task.setIsolationGroupId(taskDO.getIsolationGroupId());
        task.setIteration(taskDO.getIteration());
        task.setSubWorkflowId(taskDO.getSubWorkflowId());
        task.setSubworkflowChanged(taskDO.isSubworkflowChanged());
        return task;
    }

    /**
     * Populates the workflow input data and the tasks input/output data if stored in external
     * payload storage.
     *
     * @param workflowDO the workflowDO for which the payload data needs to be populated from
     *     external storage (if applicable)
     */
    private void populateWorkflowAndTaskPayloadData(WorkflowDO workflowDO) {
        if (StringUtils.isNotBlank(workflowDO.getExternalInputPayloadStoragePath())) {
            Map<String, Object> workflowInputParams =
                    externalPayloadStorageUtils.downloadPayload(
                            workflowDO.getExternalInputPayloadStoragePath());
            Monitors.recordExternalPayloadStorageUsage(
                    workflowDO.getWorkflowName(),
                    Operation.READ.toString(),
                    PayloadType.WORKFLOW_INPUT.toString());
            workflowDO.setInput(workflowInputParams);
            workflowDO.setExternalInputPayloadStoragePath(null);
        }

        workflowDO.getTasks().forEach(this::populateTaskData);
    }

    private void populateTaskData(TaskDO taskDO) {
        if (StringUtils.isNotBlank(taskDO.getExternalOutputPayloadStoragePath())) {
            taskDO.setOutputData(
                    externalPayloadStorageUtils.downloadPayload(
                            taskDO.getExternalOutputPayloadStoragePath()));
            Monitors.recordExternalPayloadStorageUsage(
                    taskDO.getTaskDefName(),
                    Operation.READ.toString(),
                    PayloadType.TASK_OUTPUT.toString());
            taskDO.setExternalOutputPayloadStoragePath(null);
        }

        if (StringUtils.isNotBlank(taskDO.getExternalInputPayloadStoragePath())) {
            taskDO.setInputData(
                    externalPayloadStorageUtils.downloadPayload(
                            taskDO.getExternalInputPayloadStoragePath()));
            Monitors.recordExternalPayloadStorageUsage(
                    taskDO.getTaskDefName(),
                    Operation.READ.toString(),
                    PayloadType.TASK_INPUT.toString());
            taskDO.setExternalInputPayloadStoragePath(null);
        }
    }

    private void externalizeTaskData(TaskDO taskDO) {
        externalPayloadStorageUtils.verifyAndUpload(taskDO, PayloadType.TASK_INPUT);
        externalPayloadStorageUtils.verifyAndUpload(taskDO, PayloadType.TASK_OUTPUT);
    }

    private void externalizeWorkflowData(WorkflowDO workflowDO) {
        externalPayloadStorageUtils.verifyAndUpload(workflowDO, PayloadType.WORKFLOW_INPUT);
        externalPayloadStorageUtils.verifyAndUpload(workflowDO, PayloadType.WORKFLOW_OUTPUT);
    }
}

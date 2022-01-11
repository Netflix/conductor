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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.Operation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.PayloadType;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
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

    public Workflow getWorkflowDTO(WorkflowDO workflowDO) {
        return mapToWorkflowDTO(workflowDO);
    }

    /**
     * Fetch the fully formed workflow domain object with complete payloads
     *
     * @param workflowDO the workflow domain object from the datastore
     * @return the workflow domain object {@link WorkflowDO} with payloads from external storage
     */
    public WorkflowDO getWorkflowDO(WorkflowDO workflowDO) {
        populateWorkflowAndTaskData(workflowDO);
        return workflowDO;
    }

    public Workflow mapToWorkflowDTO(WorkflowDO workflowDO) {
        externalizeWorkflowData(workflowDO);
        // TODO: taskDO => task
        workflowDO.getTasks().stream().parallel().forEach(this::externalizeTaskData);

        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatusDO.getWorkflowStatusDTO(workflowDO.getStatus()));
        workflow.setEndTime(workflowDO.getEndTime());
        workflow.setWorkflowId(workflowDO.getWorkflowId());
        workflow.setParentWorkflowId(workflowDO.getParentWorkflowId());
        workflow.setParentWorkflowTaskId(workflowDO.getParentWorkflowTaskId());
        workflow.setTasks(workflowDO.getTasks());
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
        return workflow;
    }

    private WorkflowDO populateWorkflowAndTaskData(WorkflowDO workflow) {
        //        WorkflowDO workflowInstance = workflow.copy();

        if (StringUtils.isNotBlank(workflow.getExternalInputPayloadStoragePath())) {
            // download the workflow input from external storage here and plug it into the workflow
            Map<String, Object> workflowInputParams =
                    externalPayloadStorageUtils.downloadPayload(
                            workflow.getExternalInputPayloadStoragePath());
            Monitors.recordExternalPayloadStorageUsage(
                    workflow.getWorkflowName(),
                    Operation.READ.toString(),
                    PayloadType.WORKFLOW_INPUT.toString());
            workflow.setInput(workflowInputParams);
            workflow.setExternalInputPayloadStoragePath(null);
        }

        workflow.getTasks().stream()
                .filter(
                        task ->
                                StringUtils.isNotBlank(task.getExternalInputPayloadStoragePath())
                                        || StringUtils.isNotBlank(
                                                task.getExternalOutputPayloadStoragePath()))
                .forEach(this::populateTaskData);
        return workflow;
    }

    private void populateTaskData(Task task) {
        //        if (StringUtils.isNotBlank(task.getExternalOutputPayloadStoragePath())) {
        //
        // task.setOutputData(externalPayloadStorageUtils.downloadPayload(task.getExternalOutputPayloadStoragePath()));
        //            Monitors.recordExternalPayloadStorageUsage(task.getTaskDefName(),
        // Operation.READ.toString(),
        //                PayloadType.TASK_OUTPUT.toString());
        //            task.setExternalOutputPayloadStoragePath(null);
        //        }
        //        if (StringUtils.isNotBlank(task.getExternalInputPayloadStoragePath())) {
        //
        // task.setInputData(externalPayloadStorageUtils.downloadPayload(task.getExternalInputPayloadStoragePath()));
        //            Monitors.recordExternalPayloadStorageUsage(task.getTaskDefName(),
        // Operation.READ.toString(),
        //                PayloadType.TASK_INPUT.toString());
        //            task.setExternalInputPayloadStoragePath(null);
        //        }
    }

    private void externalizeTaskData(Task task) {
        externalPayloadStorageUtils.verifyAndUpload(task, PayloadType.TASK_INPUT);
        externalPayloadStorageUtils.verifyAndUpload(task, PayloadType.TASK_OUTPUT);
    }

    private void externalizeWorkflowData(WorkflowDO workflow) {
        externalPayloadStorageUtils.verifyAndUpload(workflow, PayloadType.WORKFLOW_INPUT);
        externalPayloadStorageUtils.verifyAndUpload(workflow, PayloadType.WORKFLOW_OUTPUT);
    }
}

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
package com.netflix.conductor.domain;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

import com.google.common.base.Preconditions;

import static com.netflix.conductor.domain.WorkflowStatusDO.RUNNING;

public class WorkflowDO {

    private String ownerApp;

    private Long createdTime;

    private Long updatedTime;

    private String createdBy;

    private String updatedBy;

    private WorkflowStatusDO status = RUNNING;

    private long endTime;

    private String workflowId;

    private String parentWorkflowId;

    private String parentWorkflowTaskId;

    private List<TaskDO> tasks = new LinkedList<>();

    private Map<String, Object> input = new HashMap<>();

    private Map<String, Object> output = new HashMap<>();

    // ids 10,11 are reserved

    private String correlationId;

    private String reRunFromWorkflowId;

    private String reasonForIncompletion;

    // id 15 is reserved

    private String event;

    private Map<String, String> taskToDomain = new HashMap<>();

    private Set<String> failedReferenceTaskNames = new HashSet<>();

    private WorkflowDef workflowDefinition;

    private String externalInputPayloadStoragePath;

    private String externalOutputPayloadStoragePath;

    private int priority;

    private Map<String, Object> variables = new HashMap<>();

    private long lastRetriedTime;

    public String getOwnerApp() {
        return ownerApp;
    }

    public void setOwnerApp(String ownerApp) {
        this.ownerApp = ownerApp;
    }

    public Long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Long createdTime) {
        this.createdTime = createdTime;
    }

    public Long getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(Long updatedTime) {
        this.updatedTime = updatedTime;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public WorkflowStatusDO getStatus() {
        return status;
    }

    public void setStatus(WorkflowStatusDO status) {
        this.status = status;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getParentWorkflowId() {
        return parentWorkflowId;
    }

    public void setParentWorkflowId(String parentWorkflowId) {
        this.parentWorkflowId = parentWorkflowId;
    }

    public String getParentWorkflowTaskId() {
        return parentWorkflowTaskId;
    }

    public void setParentWorkflowTaskId(String parentWorkflowTaskId) {
        this.parentWorkflowTaskId = parentWorkflowTaskId;
    }

    public List<TaskDO> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDO> tasks) {
        this.tasks = tasks;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public Map<String, Object> getOutput() {
        return output;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getReRunFromWorkflowId() {
        return reRunFromWorkflowId;
    }

    public void setReRunFromWorkflowId(String reRunFromWorkflowId) {
        this.reRunFromWorkflowId = reRunFromWorkflowId;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = reasonForIncompletion;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Map<String, String> getTaskToDomain() {
        return taskToDomain;
    }

    public void setTaskToDomain(Map<String, String> taskToDomain) {
        this.taskToDomain = taskToDomain;
    }

    public Set<String> getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    public void setFailedReferenceTaskNames(Set<String> failedReferenceTaskNames) {
        this.failedReferenceTaskNames = failedReferenceTaskNames;
    }

    public WorkflowDef getWorkflowDefinition() {
        return workflowDefinition;
    }

    public void setWorkflowDefinition(WorkflowDef workflowDefinition) {
        this.workflowDefinition = workflowDefinition;
    }

    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    public void setExternalInputPayloadStoragePath(String externalInputPayloadStoragePath) {
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
    }

    public String getExternalOutputPayloadStoragePath() {
        return externalOutputPayloadStoragePath;
    }

    public void setExternalOutputPayloadStoragePath(String externalOutputPayloadStoragePath) {
        this.externalOutputPayloadStoragePath = externalOutputPayloadStoragePath;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public long getLastRetriedTime() {
        return lastRetriedTime;
    }

    public void setLastRetriedTime(long lastRetriedTime) {
        this.lastRetriedTime = lastRetriedTime;
    }

    /**
     * Convenience method for accessing the workflow definition name.
     *
     * @return the workflow definition name.
     */
    public String getWorkflowName() {
        Preconditions.checkNotNull(workflowDefinition, "Workflow definition is null");
        return workflowDefinition.getName();
    }

    /**
     * Convenience method for accessing the workflow definition version.
     *
     * @return the workflow definition version.
     */
    public int getWorkflowVersion() {
        Preconditions.checkNotNull(workflowDefinition, "Workflow definition is null");
        return workflowDefinition.getVersion();
    }

    public boolean hasParent() {
        return StringUtils.isNotEmpty(parentWorkflowId);
    }

    /**
     * A string representation of all relevant fields that identify this workflow. Intended for use
     * in log and other system generated messages.
     */
    public String toShortString() {
        String name = workflowDefinition != null ? workflowDefinition.getName() : null;
        Integer version = workflowDefinition != null ? workflowDefinition.getVersion() : null;
        return String.format("%s.%s/%s", name, version, workflowId);
    }

    public TaskDO getTaskByRefName(String refName) {
        if (refName == null) {
            throw new RuntimeException(
                    "refName passed is null.  Check the workflow execution.  For dynamic tasks, make sure referenceTaskName is set to a not null value");
        }
        LinkedList<TaskDO> found = new LinkedList<>();
        for (TaskDO task : tasks) {
            if (task.getReferenceTaskName() == null) {
                throw new RuntimeException(
                        "Task "
                                + task.getTaskDefName()
                                + ", seq="
                                + task.getSeq()
                                + " does not have reference name specified.");
            }
            if (task.getReferenceTaskName().equals(refName)) {
                found.add(task);
            }
        }
        if (found.isEmpty()) {
            return null;
        }
        return found.getLast();
    }

    /** @return a deep copy of the workflow instance */
    public WorkflowDO copy() {
        WorkflowDO copy = new WorkflowDO();
        copy.setInput(input);
        copy.setOutput(output);
        copy.setStatus(status);
        copy.setWorkflowId(workflowId);
        copy.setParentWorkflowId(parentWorkflowId);
        copy.setParentWorkflowTaskId(parentWorkflowTaskId);
        copy.setReRunFromWorkflowId(reRunFromWorkflowId);
        copy.setCorrelationId(correlationId);
        copy.setEvent(event);
        copy.setReasonForIncompletion(reasonForIncompletion);
        copy.setWorkflowDefinition(workflowDefinition);
        copy.setPriority(priority);
        copy.setTasks(tasks.stream().map(TaskDO::deepCopy).collect(Collectors.toList()));
        copy.setVariables(variables);
        copy.setEndTime(endTime);
        copy.setLastRetriedTime(lastRetriedTime);
        copy.setTaskToDomain(taskToDomain);
        copy.setFailedReferenceTaskNames(failedReferenceTaskNames);
        copy.setExternalInputPayloadStoragePath(externalInputPayloadStoragePath);
        copy.setExternalOutputPayloadStoragePath(externalOutputPayloadStoragePath);
        return copy;
    }
}

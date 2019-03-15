package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

import java.util.Map;

public class StartWorkflowParameters {
    String name;
    Integer version;
    WorkflowDef workflowDefinition;
    Map<String, Object> workflowInput;
    String externalInputPayloadStoragePath;
    String correlationId;
    String parentWorkflowId;
    String parentWorkflowTaskId;
    String event;
    String userDefinedId;
    Map<String, String> taskToDomain;


    public StartWorkflowParameters(String name, Integer version, WorkflowDef workflowDefinition, Map<String, Object> workflowInput, String externalInputPayloadStoragePath, String correlationId, String parentWorkflowId, String parentWorkflowTaskId, String event, String userDefinedId, Map<String, String> taskToDomain) {
        this.name = name;
        this.version = version;
        this.workflowDefinition = workflowDefinition;
        this.workflowInput = workflowInput;
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
        this.correlationId = correlationId;
        this.parentWorkflowId = parentWorkflowId;
        this.parentWorkflowTaskId = parentWorkflowTaskId;
        this.event = event;
        this.userDefinedId = userDefinedId;
        this.taskToDomain = taskToDomain;
    }

    public WorkflowDef getWorkflowDefinition() {
        return workflowDefinition;
    }

    public void setWorkflowDefinition(WorkflowDef workflowDefinition) {
        this.workflowDefinition = workflowDefinition;
    }

    public Map<String, Object> getWorkflowInput() {
        return workflowInput;
    }

    public void setWorkflowInput(Map<String, Object> workflowInput) {
        this.workflowInput = workflowInput;
    }

    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    public void setExternalInputPayloadStoragePath(String externalInputPayloadStoragePath) {
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
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

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getUserDefinedId() {
        return userDefinedId;
    }

    public void setUserDefinedId(String userDefinedId) {
        this.userDefinedId = userDefinedId;
    }

    public Map<String, String> getTaskToDomain() {
        return taskToDomain;
    }

    public void setTaskToDomain(Map<String, String> taskToDomain) {
        this.taskToDomain = taskToDomain;
    }
}

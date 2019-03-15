package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

import java.util.Map;

public class StartWorkflowParametersBuilder {
    private String name;
    private Integer version;
    private WorkflowDef workflowDefinition;
    private Map<String, Object> workflowInput;
    private String externalInputPayloadStoragePath;
    private String correlationId;
    private String parentWorkflowId;
    private String parentWorkflowTaskId;
    private String event;
    private String userDefinedId;
    private Map<String, String> taskToDomain;

    public static StartWorkflowParametersBuilder newBuilder() {
        return new StartWorkflowParametersBuilder();
    }

    public StartWorkflowParametersBuilder setWorkflowDefinition(WorkflowDef workflowDefinition) {
        this.workflowDefinition = workflowDefinition;
        return this;
    }

    public StartWorkflowParametersBuilder setWorkflowInput(Map<String, Object> workflowInput) {
        this.workflowInput = workflowInput;
        return this;
    }

    public StartWorkflowParametersBuilder setExternalInputPayloadStoragePath(String externalInputPayloadStoragePath) {
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
        return this;
    }

    public StartWorkflowParametersBuilder setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public StartWorkflowParametersBuilder setParentWorkflowId(String parentWorkflowId) {
        this.parentWorkflowId = parentWorkflowId;
        return this;
    }

    public StartWorkflowParametersBuilder setParentWorkflowTaskId(String parentWorkflowTaskId) {
        this.parentWorkflowTaskId = parentWorkflowTaskId;
        return this;
    }

    public StartWorkflowParametersBuilder setEvent(String event) {
        this.event = event;
        return this;
    }

    public StartWorkflowParametersBuilder setUserDefinedId(String userDefinedId) {
        this.userDefinedId = userDefinedId;
        return this;
    }

    public StartWorkflowParametersBuilder setTaskToDomain(Map<String, String> taskToDomain) {
        this.taskToDomain = taskToDomain;
        return this;
    }

    public StartWorkflowParametersBuilder setName(String name) {
        this.name = name;
        return this;
    }

    public StartWorkflowParametersBuilder setVersion(Integer version) {
        this.version = version;
        return this;
    }

    public StartWorkflowParameters createStartWorkflowParameters() {
        return new StartWorkflowParameters(name, version, workflowDefinition, workflowInput, externalInputPayloadStoragePath, correlationId, parentWorkflowId, parentWorkflowTaskId, event, userDefinedId, taskToDomain);
    }
}
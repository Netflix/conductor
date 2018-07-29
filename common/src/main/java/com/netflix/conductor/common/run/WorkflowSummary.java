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
package com.netflix.conductor.common.run;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;

/**
 * Captures workflow summary info to be indexed in Elastic Search.
 *
 * @author Viren
 */
public class WorkflowSummary {

    /**
     * The time should be stored as GMT
     */
    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");

    private String workflowType;

    private int version;

    private String workflowId;

    private String correlationId;

    private String startTime;

    private String updateTime;

    private String endTime;

    private WorkflowStatus status;

    private JsonNode input;

    private JsonNode output;

    private String reasonForIncompletion;

    private long executionTime;

    private String event;

    private String failedReferenceTaskNames = "";

    public WorkflowSummary() {

    }

    public WorkflowSummary(Workflow workflow) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(gmt);
        ObjectMapper objectMapper = new ObjectMapper();

        this.workflowType = workflow.getWorkflowType();
        this.version = workflow.getVersion();
        this.workflowId = workflow.getWorkflowId();
        this.correlationId = workflow.getCorrelationId();
        if (workflow.getCreateTime() != null) {
            this.startTime = sdf.format(new Date(workflow.getCreateTime()));
        }
        if (workflow.getEndTime() > 0) {
            this.endTime = sdf.format(new Date(workflow.getEndTime()));
        }
        if (workflow.getUpdateTime() != null) {
            this.updateTime = sdf.format(new Date(workflow.getUpdateTime()));
        }
        this.status = workflow.getStatus();

        if (workflow.getInput() != null) {
            JsonNode jsonNode = objectMapper.valueToTree(workflow.getInput());
            this.input = jsonNode;
        }

        if (workflow.getOutput() != null) {
            JsonNode jsonNode = objectMapper.valueToTree(workflow.getOutput());
            this.output = jsonNode;
        }

        this.reasonForIncompletion = workflow.getReasonForIncompletion();
        if (workflow.getEndTime() > 0) {
            this.executionTime = workflow.getEndTime() - workflow.getStartTime();
        }
        this.event = workflow.getEvent();
        this.failedReferenceTaskNames = workflow.getFailedReferenceTaskNames().stream()
                .collect(Collectors.joining(","));
    }

    /**
     * @return the workflowType
     */
    public String getWorkflowType() {
        return workflowType;
    }

    /**
     * @return the version
     */
    public int getVersion() {
        return version;
    }

    /**
     * @return the workflowId
     */
    public String getWorkflowId() {
        return workflowId;
    }

    /**
     * @return the correlationId
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * @return the startTime
     */
    public String getStartTime() {
        return startTime;
    }

    /**
     * @return the endTime
     */
    public String getEndTime() {
        return endTime;
    }

    /**
     * @return the status
     */
    public WorkflowStatus getStatus() {
        return status;
    }

    public JsonNode getInput() {
        return input;
    }

    public void setInput(JsonNode input) {
        this.input = input;
    }

    public JsonNode getOutput() {
        return output;
    }

    public void setOutput(JsonNode output) {
        this.output = output;
    }

    public long getOutputSize() {
        return output != null ? output.size() : 0;
    }

    /**
     * @return the reasonForIncompletion
     */
    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    /**
     * 
     * @return the executionTime
     */
    public long getExecutionTime() {
        return executionTime;
    }

    /**
     * @return the updateTime
     */
    public String getUpdateTime() {
        return updateTime;
    }

    /**
     * 
     * @return The event
     */
    public String getEvent() {
        return event;
    }

    /**
     * 
     * @param event
     *            The event
     */
    public void setEvent(String event) {
        this.event = event;
    }

    public String getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    public void setFailedReferenceTaskNames(String failedReferenceTaskNames) {
        this.failedReferenceTaskNames = failedReferenceTaskNames;
    }
}

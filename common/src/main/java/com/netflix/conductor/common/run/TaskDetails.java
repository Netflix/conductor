/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import java.util.*;


public class TaskDetails {

    public TaskDetails() {

    }

    private String workflowStatus;

    private String workflowId;

    private String correlationId;

    private String taskId;

    private String taskInput;

    private String taskOutput;

    private String taskStatus;


    /**
     * @return the status
     */
    public String getWorkflowStatus() {
        return workflowStatus;
    }

    /**
     * @param status the status to set
     */
    public void setWorkflowStatus(String workflowStatus) {
        this.workflowStatus = workflowStatus;
    }

    /**
     * @return the workflowId
     */
    public String getWorkflowId() {
        return workflowId;
    }

    /**
     * @param workflowId the workflowId to set
     */
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }


    public String getCorrelationId() {
        return correlationId;
    }


    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }


    public String getTaskId() {
        return taskId;
    }


    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }


    public String getTaskInput() {
        return taskInput;
    }

    public void setTaskInput(String taskInput) {
        this.taskInput = taskInput;
    }

    public String getTaskOutput() {
        return taskOutput;
    }

    public void setTaskOutput(String taskOutput) {
        this.taskOutput = taskOutput;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }


}

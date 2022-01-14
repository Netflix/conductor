/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.common.run;

import com.netflix.conductor.common.metadata.tasks.Task;

import java.util.*;

public class WorkflowErrorRegistry {

    private String status;

    private long startTime;

    private long endTime;

    private String workflowId;

    private String parentWorkflowId;

    private String workflowType;

    private String completeError;

    private String jobId;

    private String rankingId;

    private String orderId;

    private int errorLookUpId;

    public WorkflowErrorRegistry() {

    }

    /**
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * @return the startTime
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param startTime the startTime to set
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return the endTime
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * @param endTime the endTime to set
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * @return the duration of the workflow
     */
    public long getDuration() {
        return getEndTime() - getStartTime();
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


    /**
     *
     * @return Workflow Type / Definition
     */
    public String getWorkflowType() {
        return workflowType;
    }

    /**
     *
     * @param workflowType Workflow type
     */
    public void setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
    }


    /**
     * @return the parentWorkflowId
     */
    public String getParentWorkflowId() {
        return parentWorkflowId;
    }

    /**
     * @param parentWorkflowId the parentWorkflowId to set
     */
    public void setParentWorkflowId(String parentWorkflowId) {
        this.parentWorkflowId = parentWorkflowId;
    }


    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getRankingId() {
        return rankingId;
    }

    public void setRankingId(String rankingId) {
        this.rankingId = rankingId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCompleteError() {
        return completeError;
    }

    public void setCompleteError(String completeError) {
        this.completeError = completeError;
    }

    public int getErrorLookUpId() {
        return errorLookUpId;
    }

    public void setErrorLookUpId(int errorLookUpId) {
        this.errorLookUpId = errorLookUpId;
    }

    /**
     * @return whether this workflow is a sub-workflow.
     */
    public boolean isSubWorkflow() {
        final String parentId = getParentWorkflowId();

        return parentId != null ? !parentId.isEmpty() : false;
    }

}

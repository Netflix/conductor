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
package com.netflix.conductor.common.metadata.workflow;

import java.util.Map;

public class RerunWorkflowRequest {

	private String reRunFromWorkflowId;

	private Map<String, Object> workflowInput;
	
	private String reRunFromTaskId;

	private Map<String, Object> taskInput;

	private Map<String, Object> taskOutput;

	private String correlationId;

	private Boolean resumeParents;

	public String getReRunFromWorkflowId() {
		return reRunFromWorkflowId;
	}

	public void setReRunFromWorkflowId(String reRunFromWorkflowId) {
		this.reRunFromWorkflowId = reRunFromWorkflowId;
	}

	public Map<String, Object> getWorkflowInput() {
		return workflowInput;
	}

	public void setWorkflowInput(Map<String, Object> workflowInput) {
		this.workflowInput = workflowInput;
	}

	public String getReRunFromTaskId() {
		return reRunFromTaskId;
	}

	public void setReRunFromTaskId(String reRunFromTaskId) {
		this.reRunFromTaskId = reRunFromTaskId;
	}

	public Map<String, Object> getTaskInput() {
		return taskInput;
	}

	public void setTaskInput(Map<String, Object> taskInput) {
		this.taskInput = taskInput;
	}

	public String getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	public Boolean getResumeParents() {
		return resumeParents;
	}

	public void setResumeParents(Boolean resumeParents) {
		this.resumeParents = resumeParents;
	}

	public Map<String, Object> getTaskOutput() {
		return taskOutput;
	}

	public void setTaskOutput(Map<String, Object> taskOutput) {
		this.taskOutput = taskOutput;
	}
}

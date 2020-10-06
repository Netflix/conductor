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
package com.netflix.conductor.common.metadata.workflow;

import com.netflix.conductor.common.metadata.Auditable;

import java.util.*;

/**
 * @author Viren
 *
 */
public class WorkflowDef extends Auditable {

	private String name;
	
	private String description;
	
	private int version = 1;
	
	private LinkedList<WorkflowTask> tasks = new LinkedList<WorkflowTask>();
	
	private List<String> inputParameters = new LinkedList<String>();

	private Map<String, Object> eventMessages = new HashMap<>();

	private Map<String, String> inputValidation = new HashMap<>();

	private Map<String, Object> outputParameters = new HashMap<>();

	private Map<String, String> authValidation = new HashMap<>();

	private String failureWorkflow;

	private String cancelWorkflow;
	
	private int schemaVersion = 1;

	private List<String> retryForbidden = new LinkedList<String>();

	private String tags;

    private Integer sweepFrequency;

    /**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the tasks
	 */
	public LinkedList<WorkflowTask> getTasks() {
		return tasks;
	}

	/**
	 * @param tasks the tasks to set
	 */
	public void setTasks(LinkedList<WorkflowTask> tasks) {
		this.tasks = tasks;
	}

	/**
	 * @return the inputParameters
	 */
	public List<String> getInputParameters() {
		return inputParameters;
	}

	/**
	 * @param inputParameters the inputParameters to set
	 */
	public void setInputParameters(List<String> inputParameters) {
		this.inputParameters = inputParameters;
	}

	
	/**
	 * @return the outputParameters
	 */
	public Map<String, Object> getOutputParameters() {
		return outputParameters;
	}

	/**
	 * @param outputParameters the outputParameters to set
	 */
	public void setOutputParameters(Map<String, Object> outputParameters) {
		this.outputParameters = outputParameters;
	}
		
	/**
	 * @return the version
	 */
	public int getVersion() {
		return version;
	}

	
	/**
	 * @return the failureWorkflow
	 */
	public String getFailureWorkflow() {
		return failureWorkflow;
	}

	/**
	 * @param failureWorkflow the failureWorkflow to set
	 */
	public void setFailureWorkflow(String failureWorkflow) {
		this.failureWorkflow = failureWorkflow;
	}

	/**
	 * @return the cancelWorkflow
	 */
	public String getCancelWorkflow() {
		return cancelWorkflow;
	}

	/**
	 * @param cancelWorkflow the cancelWorkflow to set
	 */
	public void setCancelWorkflow(String cancelWorkflow) {
		this.cancelWorkflow = cancelWorkflow;
	}

	/**
	 * @param version the version to set
	 */
	public void setVersion(int version) {
		this.version = version;
	}

	
	/**
	 * @return the schemaVersion
	 */
	public int getSchemaVersion() {
		return schemaVersion;
	}

	/**
	 * @param schemaVersion the schemaVersion to set
	 */
	public void setSchemaVersion(int schemaVersion) {
		this.schemaVersion = schemaVersion;
	}


	/**
	 * @return The map describes input validation rules
	 */
	public Map<String, String> getInputValidation() {
		return inputValidation;
	}

	/**
	 * @param inputValidation the input validation rules map
	 */
	public void setInputValidation(Map<String, String> inputValidation) {
		this.inputValidation = inputValidation;
	}

	/**
	 * @return The map describes event messages to be sent for start/finish
	 */
	public Map<String, Object> getEventMessages() {
		return eventMessages;
	}

	/**
	 * @param eventMessages The map describes event messages to be sent for start/finish
	 */
	public void setEventMessages(Map<String, Object> eventMessages) {
		this.eventMessages = eventMessages;
	}

	/**
	 * @return The auth validation rules
	 */
	public Map<String, String> getAuthValidation() {
		return authValidation;
	}

	/**
	 * @param authValidation Auth validation ruels
	 */
	public void setAuthValidation(Map<String, String> authValidation) {
		this.authValidation = authValidation;
	}

	public String key(){
		return getKey(name, version);
	}
	
	public static String getKey(String name,  int version){
		return name + "." + version;
	}

	/**
	 * @return Forbidden task types for retry action
	 */
	public List<String> getRetryForbidden() {
		return retryForbidden;
	}

	/**
	 * @param retryForbidden Forbidden task types for retry action
	 */
	public void setRetryForbidden(List<String> retryForbidden) {
		this.retryForbidden = retryForbidden;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	/**
	 * @return the sweepFrequency
	 */
	public Integer getSweepFrequency() {
		return sweepFrequency;
	}

	/**
	 * @param sweepFrequency the sweepFrequency to set
	 */
	public void setSweepFrequency(Integer sweepFrequency) {
		this.sweepFrequency = sweepFrequency;
	}

    public WorkflowTask getNextTask(String taskReferenceName){
		Iterator<WorkflowTask> it = tasks.iterator();
		while(it.hasNext()){
			 WorkflowTask task = it.next();
			 WorkflowTask nextTask = task.next(taskReferenceName, null);
			 if(nextTask != null){
				 return nextTask;
			 }
			 
			 if(task.getTaskReferenceName().equals(taskReferenceName) || task.has(taskReferenceName)){
				 break;
			 }
		}
		if(it.hasNext()){
			return it.next();
		}
		return null;
	}
	
	public WorkflowTask getTaskByRefName(String taskReferenceName){
		Optional<WorkflowTask> found = all().stream().filter(wft -> wft.getTaskReferenceName().equals(taskReferenceName)).findFirst();
		if(found.isPresent()){
			return found.get();
		}
		return null;
	}
	
	public List<WorkflowTask> all(){
		List<WorkflowTask> all = new LinkedList<>();
		for(WorkflowTask wft : tasks){
			all.addAll(wft.all());
		}
		return all;
	}
}

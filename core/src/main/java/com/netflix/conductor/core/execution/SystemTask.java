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
package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.tasks.Event;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.core.execution.tasks.Wait;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Viren
 *
 */
public class SystemTask extends Task {
	
	private SystemTask(){}
	
	public static Task decisionTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input, String caseValue, List<String> caseOuput){
		SystemTask st = new SystemTask();
		st.setTaskType(SystemTaskType.DECISION.name());
		st.setTaskDefName(SystemTaskType.DECISION.name());		
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis());
		st.getInputData().put("case", caseValue);
		st.getOutputData().put("caseOutput", caseOuput);
		st.setTaskId(taskId);
		st.setStatus(Status.IN_PROGRESS);
		st.setWorkflowTask(taskToSchedule);
		return st;
	}
	
	public static Task forkTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input) {
		SystemTask st = new SystemTask();
		st.setTaskType(SystemTaskType.FORK.name());
		st.setTaskDefName(SystemTaskType.FORK.name());		
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis());
		st.setInputData(input);
		st.setTaskId(taskId);
		st.setStatus(Status.COMPLETED);
		st.setWorkflowTask(taskToSchedule);
		return st;
	}	

	public static Task forkDynamicTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, List<WorkflowTask> dynTaskList){
		SystemTask st = new SystemTask();
		st.setTaskType(SystemTaskType.FORK.name());
		st.setTaskDefName(SystemTaskType.FORK.name());		
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis()); 
		List<String> forkedTasks = dynTaskList.stream().map(t -> t.getTaskReferenceName()).collect(Collectors.toList());
		st.getInputData().put("forkedTasks", forkedTasks);
		st.getInputData().put("forkedTaskDefs", dynTaskList);	//TODO: Remove this parameter in the later releases
		st.setTaskId(taskId);
		st.setStatus(Status.COMPLETED);
		st.setWorkflowTask(taskToSchedule);
		return st;
	}	
	
	public static Task JoinTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input){
		SystemTask st = new SystemTask();
		st.setTaskType(SystemTaskType.JOIN.name());
		st.setTaskDefName(SystemTaskType.JOIN.name());
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis());
		st.setInputData(input);
		st.setTaskId(taskId);
		st.setStatus(Status.IN_PROGRESS);
		st.setWorkflowTask(taskToSchedule);
		return st;
	}	
	
	public static Task eventTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input, String sink) {
		SystemTask st = new SystemTask();
		st.setTaskType(Event.NAME);
		st.setTaskDefName(taskToSchedule.getName());
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis());
		st.setInputData(input);
		st.getInputData().put("sink", sink);
		st.setTaskId(taskId);
		st.setStatus(Status.SCHEDULED);
		st.setWorkflowTask(taskToSchedule);
		return st;
	}	
	
	public static Task waitTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input) {
		SystemTask st = new SystemTask();
		st.setTaskType(Wait.NAME);
		st.setTaskDefName(taskToSchedule.getName());
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setStartTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis());
		st.setInputData(input);
		st.setTaskId(taskId);
		st.setStatus(Status.IN_PROGRESS);
		st.setWorkflowTask(taskToSchedule);
		return st;
	}	
	
	public static Task subWorkflowTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input, String subWorkflowName, Integer subWorkflowVersion) {
		SystemTask st = new SystemTask();
		st.setTaskType(SubWorkflow.NAME);
		st.setTaskDefName(taskToSchedule.getName());
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setEndTime(System.currentTimeMillis());
		st.getInputData().put("subWorkflowName", subWorkflowName);
		st.getInputData().put("subWorkflowVersion", subWorkflowVersion);
		st.getInputData().put("workflowInput", input);
		st.setTaskId(taskId);
		st.setStatus(Status.SCHEDULED);
		st.setWorkflowTask(taskToSchedule);
		return st;

	}
	
	public static Task userDefined(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input, TaskDef taskDef, int retryCount) {
		String taskType = taskToSchedule.getType();
		SystemTask st = new SystemTask();
		st.setTaskType(taskType);
		st.setTaskDefName(taskToSchedule.getName());
		st.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		st.setWorkflowInstanceId(workflow.getWorkflowId());
		st.setCorrelationId(workflow.getCorrelationId());
		st.setScheduledTime(System.currentTimeMillis());
		st.setTaskId(taskId);
		st.setInputData(input);
		st.setStatus(Status.SCHEDULED);
	    st.setRetryCount(retryCount);
	    st.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
	    st.setWorkflowTask(taskToSchedule);
		return st;
	}
	
	public static Task createSimpleTask(Workflow workflow, String taskId, WorkflowTask taskToSchedule, Map<String, Object> input, TaskDef taskDef, int retryCount) {

		Task theTask = new Task();
		theTask.setStartDelayInSeconds(taskToSchedule.getStartDelay());		
	    theTask.setTaskId(taskId);
	    theTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
	    theTask.setInputData(input);
	    theTask.setWorkflowInstanceId(workflow.getWorkflowId());
	    theTask.setStatus(Status.SCHEDULED);
	    theTask.setTaskType(taskToSchedule.getName());
	    theTask.setTaskDefName(taskToSchedule.getName());
	    theTask.setCorrelationId(workflow.getCorrelationId());
	    theTask.setScheduledTime(System.currentTimeMillis());
	    theTask.setRetryCount(retryCount);
	    theTask.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
	    theTask.setResponseTimeoutSeconds(taskDef.getResponseTimeoutSeconds());
	    theTask.setWorkflowTask(taskToSchedule);
		return theTask;
	}
	

}

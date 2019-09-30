/*
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
package com.netflix.conductor.dao;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.run.Workflow;

import java.util.List;

/**
 * @author Viren
 * Data access layer for storing workflow executions
 */
public interface ExecutionDAO <T extends Task, W extends Workflow>  {

	/**
	 * 
	 * @param taskName Name of the task
	 * @param workflowId Workflow instance id
	 * @return List of pending tasks (in_progress) 
	 * 
	 */
	List<T> getPendingTasksByWorkflow(String taskName, String workflowId);

	/**
	 * 
	 * @param taskType Type of task
	 * @param startKey start
	 * @param count number of tasks to return
	 * @return List of tasks starting from startKey
	 * 
	 */
	List<T> getTasks(String taskType, String startKey, int count);

	/**
	 * 
	 * @param tasks tasks to be created
	 * @return List of tasks that were created.
	 * <p>
	 * <b>Note on the primary key constraint</b><p>
	 * For a given task reference name and retryCount should be considered unique/primary key.  
	 * Given two tasks with the same reference name and retryCount only one should be added to the database.
	 * </p>  
	 *  
	 */
	List<T> createTasks(List<T> tasks);

	/**
	 * 
	 * @param task Task to be updated
	 *  
	 */
	void updateTask(T task);
	
	/**
	 * Checks if the number of tasks in progress for the given taskDef will exceed the limit if the task is scheduled to be in progress (given to the worker or for system tasks start() method called)
	 * @param task The task to be executed.  Limit is set in the Task's definition 
	 * @return true if by executing this task, the limit is breached.  false otherwise.
	 * @see TaskDef#concurrencyLimit()
	 */
	boolean exceedsInProgressLimit(T task);

	/**
	 * Checks if the Task is rate limited or not based on the {@link Task#getRateLimitPerFrequency()} and {@link Task#getRateLimitFrequencyInSeconds()}
	 * @param task: which needs to be evaluated whether it is rateLimited or not
	 * @return true: If the {@link Task} is rateLimited
	 * 		false: If the {@link Task} is not rateLimited
	 */
	boolean exceedsRateLimitPerFrequency(T task);
	
	/**
	 * 
	 * @param taskId id of the task to be removed.
	 * @return true if the deletion is successful, false otherwise.
	 */
	boolean removeTask(String taskId);

	/**
	 * 
	 * @param taskId Task instance id
	 * @return Task
	 *  
	 */
	T getTask(String taskId);

	/**
	 * 
	 * @param taskIds Task instance ids
	 * @return List of tasks
	 * 
	 */
	List<T> getTasks(List<String> taskIds);
	
	/**
	 * 
	 * @param taskType Type of the task for which to retrieve the list of pending tasks
	 * @return List of pending tasks
	 * 
	 */
	List<T> getPendingTasksForTaskType(String taskType);

	/**
	 * 
	 * @param workflowId Workflow instance id
	 * @return List of tasks for the given workflow instance id
	 *  
	 */
	List<T> getTasksForWorkflow(String workflowId);
	
	/**
	 * 
	 * @param workflow Workflow to be created
	 * @return Id of the newly created workflow
	 *  
	 */
	String createWorkflow(Workflow workflow);

	/**
	 * 
	 * @param workflow Workflow to be updated
	 * @return Id of the updated workflow
	 *  
	 */
	String updateWorkflow(Workflow workflow);

	/**
	 *
	 * @param workflowId workflow instance id
	 * @return true if the deletion is successful, false otherwise
	 */
	boolean removeWorkflow(String workflowId);
	
	/**
	 * 
	 * @param workflowType Workflow Type
	 * @param workflowId workflow instance id
	 */
	void removeFromPendingWorkflow(String workflowType, String workflowId);

	/**
	 * 
	 * @param workflowId workflow instance id
	 * @return Workflow
	 *  
	 */
	W getWorkflow(String workflowId);

	/**
	 * 
	 * @param workflowId workflow instance id
	 * @param includeTasks if set, includes the tasks (pending and completed) sorted by Task Sequence number in Workflow.
	 * @return Workflow instance details
	 *  
	 */
	W getWorkflow(String workflowId, boolean includeTasks);

	/**
	 * @param workflowName name of the workflow
	 * @param version the workflow version
	 * @return List of workflow ids which are running
	 */
	List<String> getRunningWorkflowIds(String workflowName, int version);

	/**
	 * @param workflowName Name of the workflow
	 * @param version the workflow version
	 * @return List of workflows that are running
	 */
	List<W> getPendingWorkflowsByType(String workflowName, int version);

	/**
	 * 
	 * @param workflowName Name of the workflow
	 * @return No. of running workflows
	 */
	long getPendingWorkflowCount(String workflowName);

	/**
	 * 
	 * @param taskDefName Name of the task
	 * @return Number of task currently in IN_PROGRESS status
	 */
	long getInProgressTaskCount(String taskDefName);

	/**
	 * 
	 * @param workflowName Name of the workflow
	 * @param startTime epoch time 
	 * @param endTime epoch time
	 * @return List of workflows between start and end time
	 */
	List<W> getWorkflowsByType(String workflowName, Long startTime, Long endTime);

	/**
	 * 
	 * @param correlationId Correlation Id
	 * @param includeTasks Option to includeTasks in results
	 * @return List of workflows by correlation id
	 *  
	 */
	List<W> getWorkflowsByCorrelationId(String correlationId, boolean includeTasks);

	/**
	 *
	 * @return true, if the DAO implementation is capable of searching across workflows
	 * false, if the DAO implementation cannot perform searches across workflows (and needs to use indexDAO)
	 */
	boolean canSearchAcrossWorkflows();

	//Events
	
	/**
	 * 
	 * @param ee Event Execution to be stored
	 * @return true if the event was added.  false otherwise when the event by id is already already stored.
	 */
	boolean addEventExecution(EventExecution ee);
	
	/**
	 * 
	 * @param ee Event execution to be updated
	 */
	void updateEventExecution(EventExecution ee);

	/**
	 *
	 * @param ee Event execution to be removed
	 */
	void removeEventExecution(EventExecution ee);

	/**
	 * 
	 * @param eventHandlerName Name of the event handler
	 * @param eventName Event Name
	 * @param messageId ID of the message received
	 * @param max max number of executions to return
	 * @return list of matching events
	 */
	<E extends EventExecution> List<E> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max);
	
	void updateLastPoll(String taskDefName, String domain, String workerId);

	<P extends PollData> P getPollData(String taskDefName, String domain);

	<P extends PollData> List<P> getPollData(String taskDefName);

}

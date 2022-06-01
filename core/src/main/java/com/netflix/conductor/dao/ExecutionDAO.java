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
/**
 *
 */
package com.netflix.conductor.dao;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventPublished;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskDetails;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowError;
import com.netflix.conductor.common.run.WorkflowErrorRegistry;
import com.netflix.conductor.core.events.queue.Message;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Viren
 * Data access layer for storing workflow executions
 */
public interface ExecutionDAO {

    /**
     *
     * @param taskName Name of the task
     * @param workflowId Workflow instance id
     * @return List of pending tasks (in_progress)
     *
     */
    public abstract List<Task> getPendingTasksByWorkflow(String taskName, String workflowId);

    /**
     *
     * @param taskType Type of task
     * @param startKey start
     * @param count number of tasks to return
     * @return List of tasks starting from startKey
     *
     */
    public abstract List<Task> getTasks(String taskType, String startKey, int count);

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
    public abstract List<Task> createTasks(List<Task> tasks);

    /**
     *
     * @param task Task to be updated
     *
     */
    public abstract void updateTask(Task task);

    /**
     * Checks if the number of tasks in progress for the given taskDef will exceed the limit if the task is scheduled to be in progress (given to the worker or for system tasks start() method called)
     * @param task The task to be executed.  Limit is set in the Task's definition
     * @return true if by executing this task, the limit is breached.  false otherwise.
     * @see TaskDef#concurrencyLimit()
     */
    public abstract boolean exceedsInProgressLimit(Task task);

    /**
     * Checks if the Task is rate limited or not based on the {@link TaskDef#getRateLimitPerFrequency()} and {@link TaskDef#getRateLimitFrequencyInSeconds()}
     * @param task: which needs to be evaluated whether it is rateLimited or not
     * @return true: If the {@link Task} is rateLimited
     * 		false: If the {@link Task} is not rateLimited
     */
    default boolean exceedsRateLimitPerFrequency(Task task) {
        return false;
    }

    /**
     *
     * @param tasks Multiple tasks to be updated
     *
     */
    public abstract void updateTasks(List<Task> tasks);

    /**
     * Sets the in progress flag for the task
     *
     * @param task The task to set flag for
     */
    default void updateInProgressStatus(Task task) {
    }

    /**
     *
     * @param log Task Execution Log to be added
     *
     */
    public abstract void addTaskExecLog(List<TaskExecLog> log);

    /**
     *
     * @param taskId id of the task to be removed.
     *
     */
    public abstract void removeTask(String taskId);

    /**
     *
     * @param taskId Task instance id
     * @return Task
     *
     */
    public abstract Task getTask(String taskId);

    /**
     *
     * @param taskIds Task instance ids
     * @return List of tasks
     *
     */
    public abstract List<Task> getTasks(List<String> taskIds);

    /**
     *
     * @param taskType Type of the task for which to retrieve the list of pending tasks
     * @return List of pending tasks
     *
     */
    public List<Task> getPendingTasksForTaskType(String taskType);

    /**
     *
     * @param taskType System task type name (e.g. EVENt, WAIT, etc) for which to retrieve the list of pending tasks
     * @return List of pending tasks
     *
     */
    public default List<Task> getPendingSystemTasks(String taskType) {
        return Collections.emptyList();
    }

    /**
     *
     * @param workflowId Workflow instance id
     * @return List of tasks for the given workflow instance id
     *
     */
    public abstract List<Task> getTasksForWorkflow(String workflowId);

    /**
     *
     * @param workflow Workflow to be created
     * @return Id of the newly created workflow
     *
     */
    public abstract String createWorkflow(Workflow workflow);

    /**
     *
     * @param workflow Workflow to be updated
     * @return Id of the updated workflow
     *
     */
    public abstract String updateWorkflow(Workflow workflow);

    /**
     *
     * @param workflowId workflow instance id
     *
     */
    public abstract void removeWorkflow(String workflowId);

    /**
     *
     * @param workflowType Workflow Type
     * @param workflowId workflow instance id
     */
    public abstract void removeFromPendingWorkflow(String workflowType, String workflowId);

    /**
     *
     * @param workflowId workflow instance id
     * @return Workflow
     *
     */
    public abstract Workflow getWorkflow(String workflowId);

    /**
     *
     * @param workflowId workflow instance id
     * @param includeTasks if set, includes the tasks (pending and completed)
     * @return Workflow instance details
     *
     */
    public abstract Workflow getWorkflow(String workflowId, boolean includeTasks);

    /**
     *
     * @param workflowName Name of the workflow
     * @return List of workflow ids which are running
     */
    public abstract List<String> getRunningWorkflowIds(String workflowName);

    /**
     *
     * @param workflowName Name of the workflow
     * @param startTime Start time of the workflow
     * @param endTime End time of the workflow
     * @return List of workflow ids which are running
     */
    default List<String> getRunningWorkflowIds(String workflowName, String startTime, String endTime) {
        return Collections.emptyList();
    }

    default List<String> getWorkflowIdsByStartDate(String state, String workflowName, String startedBefore, String startedAfter) {
        return Collections.emptyList();
    }

    /**
     *
     * @param workflowName Name of the workflow
     * @param version Name of the workflow
     * @param startTime Start time of the workflow
     * @param endTime End time of the workflow
     * @return List of workflow ids which are running
     */
    default List<Workflow> getRunningWorkflowIds(String workflowName, Integer version, String startTime, String endTime) {
        return Collections.emptyList();
    }

    /**
     *
     * @param workflowName Name of the workflow
     * @return List of workflows that are running
     *
     */
    public abstract List<Workflow> getPendingWorkflowsByType(String workflowName);

    /**
     *
     * @param workflowName Name of the workflow
     * @return No. of running workflows
     */
    public abstract long getPendingWorkflowCount(String workflowName);

    /**
     *
     * @param taskDefName Name of the task
     * @return Number of task currently in IN_PROGRESS status
     */
    public abstract long getInProgressTaskCount(String taskDefName);

    /**
     *
     * @param workflowName Name of the workflow
     * @param startTime epoch time
     * @param endTime epoch time
     * @return List of workflows between start and end time
     */
    public abstract List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime);

    /**
     *
     * @param correlationId Correlation Id
     * @return List of workflows by correlation id
     *
     */
    public abstract List<Workflow> getWorkflowsByCorrelationId(String correlationId);


    //Events

    /**
     *
     * @param ee Event Execution to be stored
     * @return true if the event was added.  false otherwise when the event by id is already already stored.
     */
    public abstract boolean addEventExecution(EventExecution ee);

    /**
     *
     * @param ee Event execution to be updated
     */
    public abstract void updateEventExecution(EventExecution ee);

    /**
     *
     * @param eventHandlerName Name of the event handler
     * @param eventName Event Name
     * @param messageId ID of the message received
     * @param max max number of executions to return
     * @return list of matching events
     */
    public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max);

    /**
     * Adds an incoming external message into the store/index
     * @param queue Name of the registered queue
     * @param msg Message
     */
    public abstract void addMessage(String queue, Message msg);

    public abstract void updateLastPoll(String taskDefName, String domain, String workerId);

    public abstract PollData getPollData(String taskDefName, String domain);

    public abstract List<PollData> getPollData(String taskDefName);

    public abstract void addErrorRegistry(WorkflowErrorRegistry workflowErrorRegistry);

    public abstract List<WorkflowError> searchWorkflowErrorRegistry(WorkflowErrorRegistry workflowErrorRegistry);

    public abstract List<WorkflowErrorRegistry> searchWorkflowErrorRegistryList(WorkflowErrorRegistry workflowErrorRegistry);

    /**
     * Returns list of the in progress tasks associated with tags
     * @param taskType The task type currently in progress for associated workflows
     * @param tags The set of tags to search workflows
     * @return List of in progress tasks for workflows associated with tags
     */
    public default List<Task> getPendingTasksByTags(String taskType, Set<String> tags) {
        return Collections.emptyList();
    }

    public default boolean anyRunningWorkflowsByTags(Set<String> tags) {
        return false;
    }

    public default void addEventPublished(EventPublished ep) {
    }

    public default void resetStartTime(Task task, boolean updateOutput) {
    }

    public default Task getTask(String workflowId, String taskRefName) {
        throw new IllegalStateException("Not implemented");
    }

    public default void removeTask(Task task) {
        throw new IllegalStateException("Not implemented");
    }

    public default void setWorkflowAttribute(String workflowId, String name, Object value) {
        throw new IllegalStateException("Not implemented");
    }

    public abstract List<TaskDetails> searchTaskDetails(String jobId, String workflowId, String workflowType, String taskName, Boolean includeOutput);
}

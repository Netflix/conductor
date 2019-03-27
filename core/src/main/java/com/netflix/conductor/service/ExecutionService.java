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
package com.netflix.conductor.service;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.Operation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.PayloadType;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.SystemTaskType;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.Max;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 * @author visingh
 * @author Viren
 *
 */
@Singleton
@Trace
public class ExecutionService {

    private static final Logger logger = LoggerFactory.getLogger(ExecutionService.class);

    private final WorkflowExecutor workflowExecutor;
    private final ExecutionDAOFacade executionDAOFacade;
    private final MetadataDAO metadataDAO;
    private final QueueDAO queueDAO;
	private final ExternalPayloadStorage externalPayloadStorage;

    private final int taskRequeueTimeout;
    private final int maxSearchSize;

    private static final int MAX_POLL_TIMEOUT_MS = 5000;
    private static final int POLL_COUNT_ONE = 1;
    private static final int POLLING_TIMEOUT_IN_MS = 100;

    private static final int MAX_SEARCH_SIZE = 5_000;

	@Inject
	public ExecutionService(WorkflowExecutor workflowExecutor,
				ExecutionDAOFacade executionDAOFacade,
				MetadataDAO metadataDAO,
				QueueDAO queueDAO,
				Configuration config,
				ExternalPayloadStorage externalPayloadStorage) {
		this.workflowExecutor = workflowExecutor;
		this.executionDAOFacade = executionDAOFacade;
		this.metadataDAO = metadataDAO;
		this.queueDAO = queueDAO;
		this.externalPayloadStorage = externalPayloadStorage;
		this.taskRequeueTimeout = config.getIntProperty("task.requeue.timeout", 60_000);
        this.maxSearchSize = config.getIntProperty("workflow.max.search.size", 5_000);
	}

	public Task poll(String taskType, String workerId) {
		return poll(taskType, workerId, null);
	}
	public Task poll(String taskType, String workerId, String domain) {

		List<Task> tasks = poll(taskType, workerId, domain, 1, 100);
		if(tasks.isEmpty()) {
			return null;
		}
		return tasks.get(0);
	}

	public List<Task> poll(String taskType, String workerId, int count, int timeoutInMilliSecond) {
		return poll(taskType, workerId, null, count, timeoutInMilliSecond);
	}

	public List<Task> poll(String taskType, String workerId, String domain, int count, int timeoutInMilliSecond) {
		if (timeoutInMilliSecond > MAX_POLL_TIMEOUT_MS) {
			throw new ApplicationException(ApplicationException.Code.INVALID_INPUT,
					"Long Poll Timeout value cannot be more than 5 seconds");
		}
		String queueName = QueueUtils.getQueueName(taskType, domain);

		List<Task> tasks = new LinkedList<>();
		try {
			List<String> taskIds = queueDAO.pop(queueName, count, timeoutInMilliSecond);
			for (String taskId : taskIds) {
				Task task = getTask(taskId);
				if (task == null) {
					continue;
				}

				if (executionDAOFacade.exceedsInProgressLimit(task)) {
					continue;
				}

				task.setStatus(Status.IN_PROGRESS);
				if (task.getStartTime() == 0) {
					task.setStartTime(System.currentTimeMillis());
					Monitors.recordQueueWaitTime(task.getTaskDefName(), task.getQueueWaitTime());
				}
				task.setCallbackAfterSeconds(0);    // reset callbackAfterSeconds when giving the task to the worker
				task.setWorkerId(workerId);
				task.setPollCount(task.getPollCount() + 1);
				executionDAOFacade.updateTask(task);
				tasks.add(task);
			}
			executionDAOFacade.updateTaskLastPoll(taskType, domain, workerId);
			Monitors.recordTaskPoll(queueName);
		} catch (Exception e) {
			logger.error("Error polling for task: {} from worker: {} in domain: {}, count: {}", taskType, workerId, domain, count, e);
			Monitors.error(this.getClass().getCanonicalName(), "taskPoll");
		}
		return tasks;
	}

	public Task getLastPollTask(String taskType, String workerId, String domain) {
		List<Task> tasks = poll(taskType, workerId, domain, POLL_COUNT_ONE, POLLING_TIMEOUT_IN_MS);
		if (tasks.isEmpty()) {
			logger.debug("No Task available for the poll: /tasks/poll/{}?{}&{}", taskType, workerId, domain);
			return null;
		}
		Task task = tasks.get(0);
		logger.debug("The Task {} being returned for /tasks/poll/{}?{}&{}", task, taskType, workerId, domain);
		return task;
	}

	public List<PollData> getPollData(String taskType) {
		return executionDAOFacade.getTaskPollData(taskType);
	}

	public List<PollData> getAllPollData() {
		Map<String, Long> queueSizes = queueDAO.queuesDetail();
		List<PollData> allPollData = new ArrayList<>();
		queueSizes.keySet().forEach(k -> {
			try {
				if(!k.contains(QueueUtils.DOMAIN_SEPARATOR)){
					allPollData.addAll(getPollData(QueueUtils.getQueueNameWithoutDomain(k)));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		});
		return allPollData;

	}

	//For backward compatibility - to be removed in the later versions
	public void updateTask(Task task) {
		updateTask(new TaskResult(task));
	}

	public void updateTask(TaskResult taskResult) {
		workflowExecutor.updateTask(taskResult);
	}

	public List<Task> getTasks(String taskType, String startKey, int count) {
		return workflowExecutor.getTasks(taskType, startKey, count);
	}

	public Task getTask(String taskId) {
		return workflowExecutor.getTask(taskId);
	}

	public Task getPendingTaskForWorkflow(String taskReferenceName, String workflowId) {
		return workflowExecutor.getPendingTaskByWorkflow(taskReferenceName, workflowId);
	}

	public List<Task> filterTaskByTypeAndStatusForWorkflow(String workflowId, TaskType taskType, Task.Status taskStatus) {
		return workflowExecutor.filterTaskByTypeAndStatusForWorkflow(workflowId, taskType, taskStatus);
	}

	/**
	 * This method removes the task from the un-acked Queue
	 *
	 * @param taskId: the taskId that needs to be updated and removed from the unacked queue
	 * @return True in case of successful removal of the taskId from the un-acked queue
	 */
	public boolean ackTaskReceived(String taskId) {
		return Optional.ofNullable(getTask(taskId))
				.map(QueueUtils::getQueueName)
				.map(queueName -> queueDAO.ack(queueName, taskId))
				.orElse(false);
	}

	public Map<String, Integer> getTaskQueueSizes(List<String> taskDefNames) {
		Map<String, Integer> sizes = new HashMap<>();
		for (String taskDefName : taskDefNames) {
			sizes.put(taskDefName, queueDAO.getSize(taskDefName));
		}
		return sizes;
	}

	public void removeTaskfromQueue(String taskId) {
		Task task = executionDAOFacade.getTaskById(taskId);
		if (task == null) {
			throw new ApplicationException(ApplicationException.Code.NOT_FOUND,
					String.format("No such task found by taskId: %s", taskId));
		}
		queueDAO.remove(QueueUtils.getQueueName(task), taskId);
	}

	public int requeuePendingTasks() {
		long threshold = System.currentTimeMillis() - taskRequeueTimeout;
		List<WorkflowDef> workflowDefs = metadataDAO.getAll();
		int count = 0;
		for (WorkflowDef workflowDef : workflowDefs) {
			List<Workflow> workflows = workflowExecutor.getRunningWorkflows(workflowDef.getName());
			for (Workflow workflow : workflows) {
				count += requeuePendingTasks(workflow, threshold);
			}
		}
		return count;
	}

	private int requeuePendingTasks(Workflow workflow, long threshold) {

		int count = 0;
		List<Task> tasks = workflow.getTasks();
		for (Task pending : tasks) {
			if (SystemTaskType.is(pending.getTaskType())) {
				continue;
			}
			if (pending.getStatus().isTerminal()) {
				continue;
			}
			if (pending.getUpdateTime() < threshold) {
				logger.info("Requeuing Task: workflowId=" + workflow.getWorkflowId() + ", taskType=" + pending.getTaskType() + ", taskId="
						+ pending.getTaskId());
				long callback = pending.getCallbackAfterSeconds();
				if (callback < 0) {
					callback = 0;
				}
				boolean pushed = queueDAO.pushIfNotExists(QueueUtils.getQueueName(pending), pending.getTaskId(), callback);
				if (pushed) {
					count++;
				}
			}
		}
		return count;
	}

	public int requeuePendingTasks(String taskType) {

		int count = 0;
		List<Task> tasks = getPendingTasksForTaskType(taskType);

		for (Task pending : tasks) {

			if (SystemTaskType.is(pending.getTaskType())) {
				continue;
			}
			if (pending.getStatus().isTerminal()) {
				continue;
			}

			logger.info("Requeuing Task: workflowId=" + pending.getWorkflowInstanceId() + ", taskType=" + pending.getTaskType() + ", taskId=" + pending.getTaskId());
			boolean pushed = requeue(pending);
			if (pushed) {
				count++;
			}

		}
		return count;
	}

	private boolean requeue(Task pending) {
		long callback = pending.getCallbackAfterSeconds();
		if (callback < 0) {
			callback = 0;
		}
		queueDAO.remove(QueueUtils.getQueueName(pending), pending.getTaskId());
		long now = System.currentTimeMillis();
		callback = callback - ((now - pending.getUpdateTime())/1000);
		if(callback < 0) {
			callback = 0;
		}
		return queueDAO.pushIfNotExists(QueueUtils.getQueueName(pending), pending.getTaskId(), callback);
	}

	public List<Workflow> getWorkflowInstances(String workflowName, String correlationId, boolean includeClosed, boolean includeTasks) {

		List<Workflow> workflows = executionDAOFacade.getWorkflowsByCorrelationId(correlationId, includeTasks);
		List<Workflow> result = new LinkedList<>();
		for (Workflow wf : workflows) {
			if (wf.getWorkflowName().equals(workflowName) && (includeClosed || wf.getStatus().equals(Workflow.WorkflowStatus.RUNNING))) {
				result.add(wf);
			}
		}

		return result;
	}

	public Workflow getExecutionStatus(String workflowId, boolean includeTasks) {
		return executionDAOFacade.getWorkflowById(workflowId, includeTasks);
	}

	public List<String> getRunningWorkflows(String workflowName) {
		return executionDAOFacade.getRunningWorkflowIdsByName(workflowName);
	}

	public void removeWorkflow(String workflowId, boolean archiveWorkflow) {
		executionDAOFacade.removeWorkflow(workflowId, archiveWorkflow);
	}

	public SearchResult<WorkflowSummary> search(String query, String freeText, int start, int size, List<String> sortOptions) {

		SearchResult<String> result = executionDAOFacade.searchWorkflows(query, freeText, start, size, sortOptions);
		List<WorkflowSummary> workflows = result.getResults().stream().parallel().map(workflowId -> {
			try {
				return new WorkflowSummary(executionDAOFacade.getWorkflowById(workflowId,false));
			} catch(Exception e) {
				logger.error("Error fetching workflow by id: {}", workflowId, e);
				return null;
			}
		}).filter(Objects::nonNull).collect(Collectors.toList());
		int missing = result.getResults().size() - workflows.size();
		long totalHits = result.getTotalHits() - missing;
		return new SearchResult<>(totalHits, workflows);
	}

	public SearchResult<WorkflowSummary> searchWorkflowByTasks(String query, String freeText, int start, int size, List<String> sortOptions) {
		SearchResult<TaskSummary> taskSummarySearchResult = searchTasks(query, freeText, start, size, sortOptions);
		List<WorkflowSummary> workflowSummaries = taskSummarySearchResult.getResults().stream()
				.parallel()
				.map(taskSummary -> {
					try {
						String workflowId = taskSummary.getWorkflowId();
						return new WorkflowSummary(executionDAOFacade.getWorkflowById(workflowId, false));
					} catch (Exception e) {
						logger.error("Error fetching workflow by id: {}", taskSummary.getWorkflowId(), e);
						return null;
					}
				})
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
		int missing = taskSummarySearchResult.getResults().size() - workflowSummaries.size();
		long totalHits = taskSummarySearchResult.getTotalHits() - missing;
		return new SearchResult<>(totalHits, workflowSummaries);
	}

	public SearchResult<TaskSummary> searchTasks(String query, String freeText, int start, int size, List<String> sortOptions) {

		SearchResult<String> result = executionDAOFacade.searchTasks(query, freeText, start, size, sortOptions);
		List<TaskSummary> workflows = result.getResults().stream()
				.parallel()
				.map(taskId -> {
					try {
						return new TaskSummary(executionDAOFacade.getTaskById(taskId));
					} catch(Exception e) {
						logger.error(e.getMessage(), e);
						return null;
					}
				})
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
		int missing = result.getResults().size() - workflows.size();
		long totalHits = result.getTotalHits() - missing;
		return new SearchResult<>(totalHits, workflows);
	}

	public SearchResult<TaskSummary> getSearchTasks(String query, String freeText, int start,
													@Max(value = MAX_SEARCH_SIZE, message = "Cannot return more than {value} workflows." +
															" Please use pagination.") int size, String sortString) {
	    return searchTasks(query, freeText, start, size, ServiceUtils.convertStringToList(sortString));
    }

	public List<Task> getPendingTasksForTaskType(String taskType) {
		return executionDAOFacade.getPendingTasksForTaskType(taskType);
	}

	public boolean addEventExecution(EventExecution eventExecution) {
		return executionDAOFacade.addEventExecution(eventExecution);
	}

	public void removeEventExecution(EventExecution eventExecution) {
		executionDAOFacade.removeEventExecution(eventExecution);
	}

	public void updateEventExecution(EventExecution eventExecution) {
		executionDAOFacade.updateEventExecution(eventExecution);
	}

	/**
	 *
	 * @param queue Name of the registered queueDAO
	 * @param msg Message
	 */
	public void addMessage(String queue, Message msg) {
		executionDAOFacade.addMessage(queue, msg);
	}

	/**
	 * Adds task logs
	 * @param taskId Id of the task
	 * @param log logs
	 */
	public void log(String taskId, String log) {
		TaskExecLog executionLog = new TaskExecLog();
		executionLog.setTaskId(taskId);
		executionLog.setLog(log);
		executionLog.setCreatedTime(System.currentTimeMillis());
		executionDAOFacade.addTaskExecLog(Collections.singletonList(executionLog));
	}

	/**
	 *
	 * @param taskId Id of the task for which to retrieve logs
	 * @return Execution Logs (logged by the worker)
	 */
	public List<TaskExecLog> getTaskLogs(String taskId) {
		return executionDAOFacade.getTaskExecutionLogs(taskId);
	}

    /**
     * Get external uri for the payload
     *
     * @param operation the type of {@link Operation} to be performed
     * @param payloadType the {@link PayloadType} at the external uri
	 * @param path the path for which the external storage location is to be populated
     * @return the external uri at which the payload is stored/to be stored
     */
	public ExternalStorageLocation getExternalStorageLocation(Operation operation, PayloadType payloadType, String path) {
		return externalPayloadStorage.getLocation(operation, payloadType, path);
	}
}

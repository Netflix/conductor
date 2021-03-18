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
package com.netflix.conductor.service;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.SystemTaskType;
import com.netflix.conductor.core.execution.TaskStatusListener;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.reflect.Field;
import java.util.*;
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

	private WorkflowExecutor executor;

	private ExecutionDAO edao;

	private IndexDAO indexer;

	private QueueDAO queue;

	private MetadataDAO metadata;

	private int taskRequeueTimeout;
	private TaskStatusListener taskStatusListener;

	private Configuration config;

	@Inject
	public ExecutionService(WorkflowExecutor wfProvider, ExecutionDAO edao, QueueDAO queue, MetadataDAO metadata,
							IndexDAO indexer, Configuration config, TaskStatusListener taskStatusListener) {
		this.executor = wfProvider;
		this.edao = edao;
		this.queue = queue;
		this.metadata = metadata;
		this.indexer = indexer;
		this.config = config;
		this.taskRequeueTimeout = config.getIntProperty("task.requeue.timeout", 60_000);
		this.taskStatusListener = taskStatusListener;
		reloadConfig();
	}

	public Task poll(String taskType, String workerId) throws Exception {
		return poll(taskType, workerId, null);
	}

	public Task poll(String taskType, String workerId, String domain) throws Exception {

		List<Task> tasks = poll(taskType, workerId, domain, 1, 100);
		if (tasks.isEmpty()) {
			return null;
		}
		return tasks.get(0);
	}

	public List<Task> poll(String taskType, String workerId, int count, int timeoutInMilliSecond) throws Exception {
		return poll(taskType, workerId, null, count, timeoutInMilliSecond);
	}

	public List<Task> poll(String taskType, String workerId, String domain, int count, int timeoutInMilliSecond) throws Exception {

		String queueName = QueueUtils.getQueueName(taskType, domain);

		List<String> taskIds = queue.pop(queueName, count, timeoutInMilliSecond);
		if (CollectionUtils.isNotEmpty(taskIds)) {
			edao.updateLastPoll(taskType, domain, workerId);
			MetricService.getInstance().taskPoll(taskType, workerId, taskIds.size());
		}
		List<Task> tasks = new LinkedList<>();
		for (String taskId : taskIds) {
			Task task = getTask(taskId);
			if (task == null) {
				queue.remove(queueName, taskId); // We should remove the entry if no task found
				continue;
			}

			if (edao.exceedsInProgressLimit(task)) {
				MetricService.getInstance().taskRateLimited(task.getTaskType(), task.getReferenceTaskName());
				continue;
			}

			task.setStarted(true);
			task.setStatus(Status.IN_PROGRESS);
			if (task.getStartTime() == 0) {
				task.setStartTime(System.currentTimeMillis());
			}
			task.setWorkerId(workerId);
			task.setPollCount(task.getPollCount() + 1);

			// Metrics
			MetricService.getInstance().taskWait(task.getTaskType(),
				task.getReferenceTaskName(),
				task.getQueueWaitTime());

			edao.updateTask(task);
			taskStatusListener.onTaskStarted(task);
			tasks.add(task);
		}
		return tasks;
	}

	public List<PollData> getPollData(String taskType) throws Exception {
		return edao.getPollData(taskType);
	}

	public List<PollData> getAllPollData() throws Exception {
		Map<String, Long> queueSizes = queue.queuesDetail();
		List<PollData> allPollData = new ArrayList<PollData>();
		queueSizes.keySet().forEach(k -> {
			try {
				if (k.indexOf(QueueUtils.DOMAIN_SEPARATOR) == -1) {
					allPollData.addAll(getPollData(QueueUtils.getQueueNameWithoutDomain(k)));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		});
		return allPollData;

	}

	//For backward compatibility - to be removed in the later versions
	public void updateTask(Task task) throws Exception {
		updateTask(new TaskResult(task));
	}

	public void updateTask(TaskResult task) throws Exception {
		executor.updateTask(task);
	}

	public List<Task> getTasks(String taskType, String startKey, int count) throws Exception {
		return executor.getTasks(taskType, startKey, count);
	}

	public Task getTask(String taskId) throws Exception {
		return edao.getTask(taskId);
	}

	public Task getPendingTaskForWorkflow(String taskReferenceName, String workflowId) {
		return executor.getPendingTaskByWorkflow(taskReferenceName, workflowId);
	}

	public boolean ackTaskRecieved(String taskId, String consumerId) throws Exception {
		Task task = getTask(taskId);
		String queueName = QueueUtils.getQueueName(task);

		if (task != null) {
			if (task.getResponseTimeoutSeconds() > 0) {
				logger.debug("Adding task " + queueName + "/" + taskId + " to be requeued if no response received " + task.getResponseTimeoutSeconds());
				return queue.setUnackTimeout(queueName, task.getTaskId(), 1000 * task.getResponseTimeoutSeconds());        //Value is in millisecond
			} else {
				return queue.ack(queueName, taskId);
			}
		}
		return false;

	}

	public Map<String, Integer> getTaskQueueSizes(List<String> taskDefNames) {
		Map<String, Integer> sizes = new HashMap<String, Integer>();
		for (String taskDefName : taskDefNames) {
			sizes.put(taskDefName, queue.getSize(taskDefName));
		}
		return sizes;
	}

	public void removeTaskfromQueue(String taskType, String taskId) {
		Task task = edao.getTask(taskId);
		queue.remove(QueueUtils.getQueueName(task), taskId);
	}

	public int requeuePendingTasks() throws Exception {
		long threshold = System.currentTimeMillis() - taskRequeueTimeout;
		List<WorkflowDef> workflowDefs = metadata.getAll();
		int count = 0;
		for (WorkflowDef workflowDef : workflowDefs) {
			List<Workflow> workflows = executor.getRunningWorkflows(workflowDef.getName());
			for (Workflow workflow : workflows) {
				count += requeuePendingTasks(workflow, threshold);
			}
		}
		return count;
	}

	public int requeuePendingTasks(Workflow workflow, long threshold) {
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
				logger.debug("Requeuing Task: workflowId=" + workflow.getWorkflowId() + ", taskType=" + pending.getTaskType() + ", taskId="
					+ pending.getTaskId());
				long callback = pending.getCallbackAfterSeconds();
				if (callback < 0) {
					callback = 0;
				}
				boolean pushed = queue.pushIfNotExists(QueueUtils.getQueueName(pending), pending.getTaskId(), callback, workflow.getJobPriority());
				if (pushed) {
					count++;
				}
			}
		}
		return count;
	}

	public int requeuePendingTasks(String taskType) throws Exception {

		int count = 0;
		List<Task> tasks = getPendingTasksForTaskType(taskType);

		for (Task pending : tasks) {

			if (SystemTaskType.is(pending.getTaskType())) {
				continue;
			}
			if (pending.getStatus().isTerminal()) {
				continue;
			}

			logger.debug("Requeuing Task: workflowId=" + pending.getWorkflowInstanceId() + ", taskType=" + pending.getTaskType() + ", taskId=" + pending.getTaskId());
			boolean pushed = requeue(pending);
			if (pushed) {
				count++;
			}

		}
		return count;
	}

	private boolean requeue(Task pending) throws Exception {
		long callback = pending.getCallbackAfterSeconds();
		if (callback < 0) {
			callback = 0;
		}
		String queueName = QueueUtils.getQueueName(pending);
		int priority = queue.getPriority(queueName, pending.getTaskId());
		queue.remove(queueName, pending.getTaskId());
		long now = System.currentTimeMillis();
		callback = callback - ((now - pending.getUpdateTime()) / 1000);
		if (callback < 0) {
			callback = 0;
		}
		return queue.pushIfNotExists(queueName, pending.getTaskId(), callback, priority);
	}

	public List<Workflow> getWorkflowInstances(String workflowName, String correlationId, boolean includeClosed, boolean includeTasks)
		throws Exception {
		List<Workflow> workflows = executor.getStatusByCorrelationId(workflowName, correlationId, includeClosed);
		if (includeTasks) {
			workflows.forEach(wf -> {
				List<Task> tasks;
				try {
					tasks = edao.getTasksForWorkflow(wf.getWorkflowId());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				wf.setTasks(tasks);
			});
		}
		return workflows;
	}

	public Workflow getExecutionStatus(String workflowId, boolean includeTasks) {
		return edao.getWorkflow(workflowId, includeTasks);
	}

	public List<String> getRunningWorkflows(String workflowName) {
		return edao.getRunningWorkflowIds(workflowName);
	}

	public void removeWorkflow(String workflowId) throws Exception {
		edao.removeWorkflow(workflowId);
	}

	public SearchResult<WorkflowSummary> search(String query, String freeText, int start, int size, List<String> sortOptions, String from, String end) {

		SearchResult<String> result = indexer.searchWorkflows(query, freeText, start, size, sortOptions, from, end);
		List<WorkflowSummary> workflows = result.getResults().stream().parallel().map(workflowId -> {
			try {
				Workflow workflow = edao.getWorkflow(workflowId, false);
				if (workflow == null)
					return null;

				return new WorkflowSummary(workflow);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				return null;
			}
		}).filter(Objects::nonNull).collect(Collectors.toList());
		int missing = result.getResults().size() - workflows.size();
		long totalHits = result.getTotalHits() - missing;

		return new SearchResult<>(totalHits, workflows);
	}

	public List<Task> getPendingTasksForTaskType(String taskType) throws Exception {
		return edao.getPendingTasksForTaskType(taskType);
	}

	public boolean addEventExecution(EventExecution ee) {
		return edao.addEventExecution(ee);
	}


	public void updateEventExecution(EventExecution ee) {
		edao.updateEventExecution(ee);
	}

	/**
	 *
	 * @param queue Name of the registered queue
	 * @param msg Message
	 */
	public void addMessage(String queue, Message msg) {
		edao.addMessage(queue, msg);
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
		edao.addTaskExecLog(Arrays.asList(executionLog));
	}

	/**
	 *
	 * @param taskId Id of the task for which to retrieve logs
	 * @return Execution Logs (logged by the worker)
	 */
	public List<TaskExecLog> getTaskLogs(String taskId) {
		return indexer.getTaskLogs(taskId);
	}

	public void reloadConfig() {
		List<Pair<String, String>> pairs = metadata.getConfigs();
		pairs.forEach(pair -> {
			config.override(pair.getKey(), pair.getValue());
			if (pair.getKey().startsWith("log4j_logger_")) {
				String name = pair.getKey().replace("log4j_logger_", "").replaceAll("_", ".");
				Level targetLevel = Level.toLevel(pair.getValue());

				try {
					setLevel(name, targetLevel);
				} catch (Exception e) {
					logger.error("set log level failed with {} for {}", e.getMessage(), pair.toString(), e);
				}
			}
		});
	}

	private void setLevel(String name, Level level) throws Exception {
		// Get slf4j wrapper
		Logger slfLogger = LoggerFactory.getLogger(name);
		Field field = slfLogger.getClass().getDeclaredField("logger");
		field.setAccessible(true);

		// Ge actual log4j logger
		org.apache.log4j.Logger target = (org.apache.log4j.Logger) field.get(slfLogger);
		target.setLevel(level);

		// Apache common logging to log4j logger
		Log4JLogger commLogger = (Log4JLogger) LogFactory.getLog(name);
		commLogger.getLogger().setLevel(level);
	}


	public boolean anyRunningWorkflowsByTags(Set<String> tags) {
		return edao.anyRunningWorkflowsByTags(tags);
	}
}

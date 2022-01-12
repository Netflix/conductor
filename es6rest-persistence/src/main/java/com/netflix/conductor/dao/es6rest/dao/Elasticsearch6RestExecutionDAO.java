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
package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventPublished;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskDetails;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowError;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.common.run.WorkflowErrorRegistry;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.index.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class Elasticsearch6RestExecutionDAO extends Elasticsearch6RestAbstractDAO implements ExecutionDAO {
	private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6RestExecutionDAO.class);
	private static final String ARCHIVED_FIELD = "archived";
	private static final String RAW_JSON_FIELD = "rawJSON";
	// Keys Families
	private final static String IN_PROGRESS_TASKS = "IN_PROGRESS_TASKS";
	private final static String WORKFLOW_TO_TASKS = "WORKFLOW_TO_TASKS";
	private final static String SCHEDULED_TASKS = "SCHEDULED_TASKS";
	private final static String TASK = "TASK";
	private final static String WORKFLOW = "WORKFLOW";
	private final static String PENDING_WORKFLOWS = "PENDING_WORKFLOWS";
	private final static String WORKFLOW_DEF_TO_WORKFLOWS = "WORKFLOW_DEF_TO_WORKFLOWS";
	private final static String CORR_ID_TO_WORKFLOWS = "CORRID_TO_WORKFLOW";
	private final static String POLL_DATA = "POLL_DATA";
	private final static String EVENT_EXECUTION = "EVENT_EXECUTION";
	private final static String EVENT_PUBLISHED = "EVENT_PUBLISHED";
	private final static String WORKFLOW_TAGS = "WORKFLOW_TAGS";

	// static indexes and types
	private final Map<String, String> indexes = new HashMap<>();
	private final Map<String, String> types = new HashMap<>();

	private MetadataDAO metadata;
	private IndexDAO indexer;

	@Inject
	public Elasticsearch6RestExecutionDAO(RestClientBuilder builder, Configuration config, ObjectMapper mapper, IndexDAO indexer, MetadataDAO metadata) {
		super(builder, config, mapper, "runtime");
		this.indexer = indexer;
		this.metadata = metadata;

		// Rest of static indexes
		initIndexTypeNames(IN_PROGRESS_TASKS);
		initIndexTypeNames(WORKFLOW_TO_TASKS);
		initIndexTypeNames(SCHEDULED_TASKS);
		initIndexTypeNames(TASK);
		initIndexTypeNames(WORKFLOW);
		initIndexTypeNames(PENDING_WORKFLOWS);
		initIndexTypeNames(WORKFLOW_DEF_TO_WORKFLOWS);
		initIndexTypeNames(CORR_ID_TO_WORKFLOWS);
		initIndexTypeNames(POLL_DATA);
		initIndexTypeNames(EVENT_EXECUTION);
		initIndexTypeNames(EVENT_PUBLISHED);
		initIndexTypeNames(WORKFLOW_TAGS);

		// Explicitly init these indexes
		ensureIndexExists(indexes.get(IN_PROGRESS_TASKS));
		ensureIndexExists(indexes.get(WORKFLOW_TO_TASKS));
		ensureIndexExists(indexes.get(SCHEDULED_TASKS));
		ensureIndexExists(indexes.get(PENDING_WORKFLOWS));
		ensureIndexExists(indexes.get(WORKFLOW_DEF_TO_WORKFLOWS));
		ensureIndexExists(indexes.get(CORR_ID_TO_WORKFLOWS));
		ensureIndexExists(indexes.get(POLL_DATA));
		ensureIndexExists(indexes.get(WORKFLOW_TAGS));

		// Explicitly init these indexes as they are using `es6runtime_***.json` resource file
		ensureIndexExists(indexes.get(TASK), types.get(TASK));
		ensureIndexExists(indexes.get(WORKFLOW), types.get(WORKFLOW));
		ensureIndexExists(indexes.get(EVENT_EXECUTION), types.get(EVENT_EXECUTION));
		ensureIndexExists(indexes.get(EVENT_PUBLISHED), types.get(EVENT_PUBLISHED));
	}

	private void initIndexTypeNames(String name) {
		indexes.put(name, toIndexName(name));
		types.put(name, toTypeName(name));
	}

	@Override
	public List<Task> getPendingTasksByWorkflow(String taskName, String workflowId) {
		if (logger.isDebugEnabled())
			logger.debug("getPendingTasksByWorkflow: taskName={}, workflowId={}", taskName, workflowId);

		QueryBuilder query = QueryBuilders.termQuery("taskDefName.keyword", taskName);
		List<HashMap> wraps = findAll(indexes.get(IN_PROGRESS_TASKS), types.get(IN_PROGRESS_TASKS), query, HashMap.class);
		Set<String> taskIds = wraps.stream().filter(map -> workflowId.equals(map.get("workflowId")))
			.map(map -> (String) map.get("taskId"))
			.collect(Collectors.toSet());
		List<Task> tasks = taskIds.stream().map(this::getTask).filter(Objects::nonNull).collect(Collectors.toList());

		if (logger.isDebugEnabled())
			logger.debug("getPendingTasksByWorkflow: result={}", toJson(tasks));
		return tasks;
	}

	@Override
	public List<Task> getTasks(String taskDefName, String startKey, int count) {
		if (logger.isDebugEnabled())
			logger.debug("getTasks: taskDefName={}, startKey={}, count={}", taskDefName, startKey, count);
		List<Task> tasks = Lists.newLinkedList();

		List<Task> pendingTasks = getPendingTasksForTaskType(taskDefName);
		boolean startKeyFound = startKey == null;
		int foundCount = 0;
		for (Task pendingTask : pendingTasks) {
			if (!startKeyFound) {
				if (pendingTask.getTaskId().equals(startKey)) {
					startKeyFound = true;
					continue;
				}
			}
			if (startKeyFound && foundCount < count) {
				tasks.add(pendingTask);
				foundCount++;
			}
		}
		return tasks;
	}

	@Override
	public List<Task> createTasks(List<Task> tasks) {
		if (logger.isDebugEnabled())
			logger.debug("createTasks: tasks={}", toJson(tasks));
		List<Task> created = Lists.newLinkedList();

		for (Task task : tasks) {

			Preconditions.checkNotNull(task, "task object cannot be null");
			Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
			Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
			Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");

			task.setScheduledTime(System.currentTimeMillis());

			boolean scheduledTaskAdded = addScheduledTask(task);
			if (!scheduledTaskAdded) {
				String taskKey = task.getReferenceTaskName() + "" + task.getRetryCount();
				if (logger.isDebugEnabled())
					logger.debug("Task already scheduled, skipping the run " + task.getTaskId() + ", ref=" + task.getReferenceTaskName() + ", key=" + taskKey);
				continue;
			}
			insertOrUpdateTask(task);
			addTaskToWorkflowMapping(task);
			addTaskInProgress(task);
			updateTask(task);

			created.add(task);
		}

		return created;
	}

	@Override
	public void updateTask(Task task) {
		if (logger.isDebugEnabled())
			logger.debug("updateTask: task={}", toJson(task));
		task.setUpdateTime(System.currentTimeMillis());
		if (task.getStatus() != null && task.getStatus().isTerminal()) {
			task.setEndTime(System.currentTimeMillis());
		}

		TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());

		if (taskDef != null && taskDef.concurrencyLimit() > 0) {
			if (task.getStatus() != null && task.getStatus().equals(Task.Status.IN_PROGRESS)) {
				addTaskInProgress(task);
			} else {
				deleteTaskInProgress(task);
			}
		}

		insertOrUpdateTask(task);
		if (task.getStatus() != null && task.getStatus().isTerminal()) {
			deleteTaskInProgress(task);
		}

		indexer.index(task);
	}

	@Override
	public void resetStartTime(Task task, boolean updateOutput) {
		String indexName = indexes.get(TASK);
		String typeName = types.get(TASK);
		String id = toId(task.getTaskId());

		Map<String, Object> payload = new HashMap<>();
		payload.put("startTime", task.getStartTime());
		payload.put("endTime", task.getEndTime());
		if (updateOutput) {
			payload.put("outputData", task.getOutputData());
		}

		merge(indexName, typeName, id, payload);
	}

	@Override
	public boolean exceedsInProgressLimit(Task task) {
		if (logger.isDebugEnabled())
			logger.debug("exceedsInProgressLimit: task={}", toJson(task));

		TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());
		if (taskDef == null) {
			return false;
		}
		int limit = taskDef.concurrencyLimit();
		if (limit <= 0) {
			return false;
		}

		long current = getInProgressTaskCount(task.getTaskDefName());
		if (current >= limit) {
			if (logger.isDebugEnabled())
				logger.debug("exceedsInProgressLimit: task rate limited. current={}, limit={}", current, limit);
			Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
			return true;
		}

		String indexName = indexes.get(IN_PROGRESS_TASKS);
		String typeName = types.get(IN_PROGRESS_TASKS);
		QueryBuilder query = QueryBuilders.termQuery("taskDefName.keyword", task.getTaskDefName());
		List<HashMap> wraps = findAll(indexName, typeName, query, limit, HashMap.class);
		Set<String> ids = wraps.stream().map(map -> (String) map.get("taskId")).collect(Collectors.toSet());
		if (logger.isDebugEnabled())
			logger.debug("exceedsInProgressLimit: ids={}", ids);

		if (ids.isEmpty()) {
			if (logger.isDebugEnabled())
				logger.debug("exceedsInProgressLimit: Task execution not limited for {}", task.getTaskDefName());
			return false;
		}

		boolean rateLimited = !ids.contains(task.getTaskId());
		if (rateLimited) {
			logger.debug("Task execution count limited. {}, limit {}, current {}", task.getTaskDefName(), limit, current);
			Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
		}

		if (logger.isDebugEnabled())
			logger.debug("exceedsInProgressLimit: result={}", rateLimited);
		return rateLimited;
	}

	@Override
	public void addTaskExecLog(List<TaskExecLog> log) {
		if (logger.isDebugEnabled())
			logger.debug("addTaskExecLog: log={}", toJson(log));
		indexer.add(log);
	}

	@Override
	public void updateTasks(List<Task> tasks) {
		if (logger.isDebugEnabled())
			logger.debug("updateTasks: tasks={}", toJson(tasks));
		for (Task task : tasks) {
			updateTask(task);
		}
	}

	@Override
	public void removeTask(String taskId) {
		if (logger.isDebugEnabled())
			logger.debug("removeTask: taskId={}", taskId);
		Task task = getTask(taskId);
		if (task == null) {
			logger.warn("No such Task by id {}", taskId);
			return;
		}
		deleteScheduledTask(task);
		deleteTaskInProgress(task);
		deleteTaskToWorkflowMapping(task);
		deleteTask(task);
		if (logger.isDebugEnabled())
			logger.debug("removeTask: done");
	}

	@Override
	public Task getTask(String taskId) {
		if (logger.isDebugEnabled())
			logger.debug("getTask: taskId={}", taskId);
		Preconditions.checkNotNull(taskId, "taskId name cannot be null");

		Task task = findOne(indexes.get(TASK), types.get(TASK), toId(taskId), Task.class);

		if (logger.isDebugEnabled())
			logger.debug("getTask: result={}", toJson(task));
		return task;
	}

	@Override
	public Task getTask(String workflowId, String taskRefName) {
		QueryBuilder termTaskRefName = QueryBuilders.termQuery("referenceTaskName", taskRefName);
		QueryBuilder termWorkflowId = QueryBuilders.termQuery("workflowInstanceId", workflowId);
		QueryBuilder query = QueryBuilders.boolQuery().must(termWorkflowId).must(termTaskRefName);

		List<Task> tasks = findAll(indexes.get(TASK), types.get(TASK), query, 1, Task.class);
		if (tasks.isEmpty())
			return null;
		return tasks.get(0);
	}

	@Override
	public List<Task> getTasks(List<String> taskIds) {
		if (logger.isDebugEnabled())
			logger.debug("getTasks: taskIds={}", taskIds);

		IdsQueryBuilder idsQuery = QueryBuilders.idsQuery();
		taskIds.forEach(id -> idsQuery.addIds(toId(id)));

		List<Task> tasks = findAll(indexes.get(TASK), types.get(TASK), idsQuery, Task.class);

		if (logger.isDebugEnabled())
			logger.debug("getTasks: result={}", toJson(tasks));
		return tasks;
	}

	@Override
	public List<Task> getPendingTasksForTaskType(String taskDefName) {
		if (logger.isDebugEnabled())
			logger.debug("getPendingTasksForTaskType: taskDefName={}", taskDefName);
		Preconditions.checkNotNull(taskDefName, "task def name cannot be null");

		QueryBuilder query = QueryBuilders.termQuery("taskDefName.keyword", taskDefName);
		List<HashMap> wraps = findAll(indexes.get(IN_PROGRESS_TASKS), types.get(IN_PROGRESS_TASKS), query, HashMap.class);
		Set<String> taskIds = wraps.stream().map(map -> (String) map.get("taskId")).collect(Collectors.toSet());
		List<Task> tasks = taskIds.stream().map(this::getTask).filter(Objects::nonNull).collect(Collectors.toList());

		if (logger.isDebugEnabled())
			logger.debug("getPendingTasksForTaskType: result={}", toJson(tasks));
		return tasks;
	}

	@Override
	public List<Task> getPendingSystemTasks(String taskType) {
		if (logger.isDebugEnabled())
			logger.debug("getPendingSystemTasks: taskName={}", taskType);
		Preconditions.checkNotNull(taskType, "task type cannot be null");

		QueryBuilder termType = QueryBuilders.termQuery("taskType", taskType);
		QueryBuilder termStatus = QueryBuilders.termQuery("status", "IN_PROGRESS");
		QueryBuilder query = QueryBuilders.boolQuery().must(termType).must(termStatus);

		return findAll(indexes.get(TASK), types.get(TASK), query, Task.class);
	}

	@Override
	public List<Task> getTasksForWorkflow(String workflowId) {
		if (logger.isDebugEnabled())
			logger.debug("getTasksForWorkflow: workflowId={}", workflowId);
		Preconditions.checkNotNull(workflowId, "workflowId cannot be null");

		QueryBuilder query = QueryBuilders.termQuery("workflowId.keyword", workflowId);
		List<HashMap> wraps = findAll(indexes.get(WORKFLOW_TO_TASKS), types.get(WORKFLOW_TO_TASKS), query, HashMap.class);
		Set<String> taskIds = wraps.stream().map(map -> (String) map.get("taskId")).collect(Collectors.toSet());
		List<Task> tasks = taskIds.stream().map(this::getTask).filter(Objects::nonNull).collect(Collectors.toList());

		if (logger.isDebugEnabled())
			logger.debug("getTasksForWorkflow: result={}", toJson(tasks));
		return tasks;
	}

	@Override
	public String createWorkflow(Workflow workflow) {
		if (logger.isDebugEnabled())
			logger.debug("createWorkflow: workflow={}", toJson(workflow));
		workflow.setCreateTime(System.currentTimeMillis());
		return insertOrUpdateWorkflow(workflow, false);
	}

	@Override
	public String updateWorkflow(Workflow workflow) {
		if (logger.isDebugEnabled())
			logger.debug("updateWorkflow: workflow={}", toJson(workflow));
		workflow.setUpdateTime(System.currentTimeMillis());
		return insertOrUpdateWorkflow(workflow, true);
	}

	@Override
	public void removeWorkflow(String workflowId) {
		if (logger.isDebugEnabled())
			logger.debug("removeWorkflow: workflowId={}", workflowId);
		try {

			Workflow wf = getWorkflow(workflowId, true);

			//Add to elasticsearch
			indexer.update(workflowId, new String[]{RAW_JSON_FIELD, ARCHIVED_FIELD}, new Object[]{toJson(wf), true});
			deleteWorkflowDefToWorkflowMapping(wf);
			deleteWorkflowToCorrIdMapping(wf);
			deletePendingWorkflow(wf);
			deleteWorkflow(wf);

			for (Task task : wf.getTasks()) {
				removeTask(task.getTaskId());
			}
		} catch (Exception ex) {
			if (logger.isDebugEnabled())
				logger.debug("removeWorkflow: failed for {} with {}", workflowId, ex.getMessage(), ex);
			throw new ApplicationException(ex.getMessage(), ex);
		}
		if (logger.isDebugEnabled())
			logger.debug("removeWorkflow: done");
	}

	@Override
	public void removeFromPendingWorkflow(String workflowType, String workflowId) {
		if (logger.isDebugEnabled())
			logger.debug("removeFromPendingWorkflow: workflowType={}, workflowId={}", workflowType, workflowId);

		deletePendingWorkflow(workflowType, workflowId);

		if (logger.isDebugEnabled())
			logger.debug("removeFromPendingWorkflow: done");
	}

	@Override
	public Workflow getWorkflow(String workflowId) {
		if (logger.isDebugEnabled())
			logger.debug("getWorkflow: workflowId={}", workflowId);
		return getWorkflow(workflowId, true);
	}

	@Override
	public Workflow getWorkflow(String workflowId, boolean includeTasks) {
		if (logger.isDebugEnabled())
			logger.debug("getWorkflow: workflowId={}, includeTasks={}", workflowId, includeTasks);

		Workflow workflow = findOne(indexes.get(WORKFLOW), types.get(WORKFLOW), toId(workflowId), Workflow.class);
		if (workflow != null) {
			if (includeTasks) {
				List<Task> tasks = getTasksForWorkflow(workflowId);
				tasks.sort(Comparator.comparingLong(Task::getScheduledTime).thenComparingInt(Task::getSeq));
				workflow.setTasks(tasks);
			}

			if (logger.isDebugEnabled())
				logger.debug("getWorkflow: result(1)={}", toJson(workflow));
			return workflow;
		}

		// try from the archive
		String json = indexer.get(workflowId, RAW_JSON_FIELD);
		if (json == null) {
			if (logger.isDebugEnabled())
				logger.debug("getWorkflow: No such workflow found by id: " + workflowId);
			return null;
		}
		workflow = convert(json, Workflow.class);
		if (!includeTasks) {
			workflow.getTasks().clear();
		}

		if (logger.isDebugEnabled())
			logger.debug("getWorkflow: result(2)={}", toJson(workflow));
		return workflow;
	}

	@Override
	public List<String> getRunningWorkflowIds(String workflowName) {
		if (logger.isDebugEnabled())
			logger.debug("getRunningWorkflowIds: workflowName={}", workflowName);
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");

		QueryBuilder query = QueryBuilders.termQuery("workflowType.keyword", workflowName);
		List<HashMap> wraps = findAll(indexes.get(PENDING_WORKFLOWS), types.get(PENDING_WORKFLOWS), query, HashMap.class);
		Set<String> workflowIds = wraps.stream().map(map -> (String) map.get("workflowId"))
			.filter(Objects::nonNull).collect(Collectors.toSet());

		if (logger.isDebugEnabled())
			logger.debug("getRunningWorkflowIds: result={}", workflowIds);
		return Lists.newArrayList(workflowIds);
	}

	@Override
	public List<Workflow> getPendingWorkflowsByType(String workflowName) {
		if (logger.isDebugEnabled())
			logger.debug("getPendingWorkflowsByType: workflowName={}", workflowName);
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");

		List<String> wfIds = getRunningWorkflowIds(workflowName);
		List<Workflow> workflows = wfIds.stream().map(this::getWorkflow).filter(Objects::nonNull)
			.collect(Collectors.toList());

		if (logger.isDebugEnabled())
			logger.debug("getPendingWorkflowsByType: result={}", toJson(workflows));
		return workflows;
	}

	@Override
	public long getPendingWorkflowCount(String workflowName) {
		if (logger.isDebugEnabled())
			logger.debug("getPendingWorkflowCount: workflowName={}", workflowName);

		String indexName = indexes.get(PENDING_WORKFLOWS);
		ensureIndexExists(indexName);

		QueryBuilder query = QueryBuilders.termQuery("workflowType.keyword", workflowName);

		long result = getCount(indexName, null, query);

		if (logger.isDebugEnabled())
			logger.debug("getPendingWorkflowCount: result={}", result);

		return result;
	}

	@Override
	public long getInProgressTaskCount(String taskDefName) {
		if (logger.isDebugEnabled())
			logger.debug("getInProgressTaskCount: taskDefName={}", taskDefName);

		String indexName = indexes.get(IN_PROGRESS_TASKS);
		String typeName = types.get(IN_PROGRESS_TASKS);
		ensureIndexExists(indexName);

		QueryBuilder query = QueryBuilders.termQuery("taskDefName.keyword", taskDefName);

		long result = getCount(indexName, typeName, query);

		if (logger.isDebugEnabled())
			logger.debug("getInProgressTaskCount: result={}", result);
		return result;
	}

	@Override
	public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
		if (logger.isDebugEnabled())
			logger.debug("getWorkflowsByType: workflowName={}, startTime={}, endTime={}", workflowName, startTime, endTime);

		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		Preconditions.checkNotNull(startTime, "startTime cannot be null");
		Preconditions.checkNotNull(endTime, "endTime cannot be null");

		List<Workflow> workflows = Lists.newLinkedList();

		List<String> dateStrs = dateStrBetweenDates(startTime, endTime);
		dateStrs.forEach(dateStr -> {
			QueryBuilder query1 = QueryBuilders
				.termQuery("workflowType.keyword", workflowName);

			QueryBuilder query2 = QueryBuilders
				.termQuery("dateStr.keyword", dateStr);

			QueryBuilder query = QueryBuilders.boolQuery().must(query1).must(query2);

			List<HashMap> wraps = findAll(indexes.get(WORKFLOW_DEF_TO_WORKFLOWS), types.get(WORKFLOW_DEF_TO_WORKFLOWS), query, HashMap.class);
			Set<String> workflowIds = wraps.stream().map(map -> (String) map.get("workflowId")).collect(Collectors.toSet());
			workflowIds.forEach(wfId -> {
				try {
					Workflow wf = getWorkflow(wfId);
					if (wf.getCreateTime() >= startTime && wf.getCreateTime() <= endTime) {
						workflows.add(wf);
					}
				} catch (Exception ex) {
					logger.error("getWorkflowsByType: Unable to find {} workflow {}", wfId, ex.getMessage(), ex);
				}
			});
		});

		if (logger.isDebugEnabled())
			logger.debug("getWorkflowsByType: result={}", toJson(workflows));
		return workflows;
	}

	@Override
	public List<Workflow> getWorkflowsByCorrelationId(String correlationId) {
		if (logger.isDebugEnabled())
			logger.debug("getWorkflowsByCorrelationId: correlationId={}", correlationId);

		Preconditions.checkNotNull(correlationId, "correlationId cannot be null");

		String sha256hex = DigestUtils.sha256Hex(correlationId);
		QueryBuilder query = QueryBuilders.termQuery("sha256hex.keyword", sha256hex);
		List<HashMap> wraps = findAll(indexes.get(CORR_ID_TO_WORKFLOWS), types.get(CORR_ID_TO_WORKFLOWS), query, HashMap.class);
		Set<String> workflowIds = wraps.stream().map(map -> (String) map.get("workflowId")).collect(Collectors.toSet());
		List<Workflow> workflows = workflowIds.stream().map(this::getWorkflow).collect(Collectors.toList());

		if (logger.isDebugEnabled())
			logger.debug("getWorkflowsByCorrelationId: result={}", toJson(workflows));
		return workflows;
	}

	@Override
	public boolean addEventExecution(EventExecution ee) {
		if (logger.isDebugEnabled())
			logger.debug("addEventExecution: ee={}", toJson(ee));
		try {
			String id = toId(ee.getName(), ee.getEvent(), ee.getMessageId(), ee.getId());

			if (insert(indexes.get(EVENT_EXECUTION), types.get(EVENT_EXECUTION), id, ee)) {
				indexer.add(ee);

				if (logger.isDebugEnabled())
					logger.debug("addEventExecution: true");
				return true;
			}

			if (logger.isDebugEnabled())
				logger.debug("addEventExecution: false");
			return false;
		} catch (Exception ex) {
			if (logger.isDebugEnabled())
				logger.debug("addEventExecution: failed with {}", ex.getMessage());
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
		}
	}

	@Override
	public void updateEventExecution(EventExecution ee) {
		if (logger.isDebugEnabled())
			logger.debug("updateEventExecution: ee={}", toJson(ee));
		try {
			String id = toId(ee.getName(), ee.getEvent(), ee.getMessageId(), ee.getId());

			upsert(indexes.get(EVENT_EXECUTION), types.get(EVENT_EXECUTION), id, ee);

			indexer.add(ee);
			if (logger.isDebugEnabled())
				logger.debug("updateEventExecution: done");
		} catch (Exception ex) {
			if (logger.isDebugEnabled())
				logger.debug("updateEventExecution: failed with {}", ex.getMessage());
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
		}
	}

	@Override
	public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max) {
		if (logger.isDebugEnabled())
			logger.debug("getEventExecutions: eventHandlerName={}, eventName={}, messageId={}, max={}",
				eventHandlerName, eventName, messageId, max);
		try {
			List<EventExecution> executions = Lists.newLinkedList();
			for (int i = 0; i < max; i++) {
				String id = toId(eventHandlerName, eventName, messageId, messageId + "_" + i);
				EventExecution ee = findOne(indexes.get(EVENT_EXECUTION), types.get(EVENT_EXECUTION), id, EventExecution.class);
				if (ee == null) {
					break;
				}
				executions.add(ee);
			}

			if (logger.isDebugEnabled())
				logger.debug("getEventExecutions: result={}", toJson(executions));
			return executions;
		} catch (Exception ex) {
			if (logger.isDebugEnabled())
				logger.debug("getEventExecutions: failed with {}", ex.getMessage());
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
		}
	}

	@Override
	public void addMessage(String queue, Message msg) {
		if (logger.isDebugEnabled())
			logger.debug("addMessage: queue={}, msg={}", queue, toJson(msg));

		indexer.addMessage(queue, msg);
	}

	@Override
	public void updateLastPoll(String queueName, String domain, String workerId) {
		if (logger.isDebugEnabled())
			logger.debug("updateLastPoll: queueName={}, domain={}, workerId={}", queueName, domain, workerId);

		Preconditions.checkNotNull(queueName, "queueName name cannot be null");
		PollData pollData = new PollData(queueName, domain, workerId, System.currentTimeMillis());

		String field = (domain == null) ? "DEFAULT" : domain;
		String id = toId(queueName, field);

		upsert(indexes.get(POLL_DATA), types.get(POLL_DATA), id, pollData);

		if (logger.isDebugEnabled())
			logger.debug("updateLastPoll: done");
	}

	@Override
	public PollData getPollData(String queueName, String domain) {
		if (logger.isDebugEnabled())
			logger.debug("getPollData: queueName={}, domain={}", queueName, domain);

		Preconditions.checkNotNull(queueName, "queueName name cannot be null");

		String field = (domain == null) ? "DEFAULT" : domain;
		String id = toId(queueName, field);

		PollData pollData = findOne(indexes.get(POLL_DATA), types.get(POLL_DATA), id, PollData.class);

		if (logger.isDebugEnabled())
			logger.debug("getPollData: result={}", toJson(pollData));
		return pollData;
	}

	@Override
	public List<PollData> getPollData(String queueName) {
		if (logger.isDebugEnabled())
			logger.debug("getPollData: queueName={}", queueName);
		Preconditions.checkNotNull(queueName, "queueName name cannot be null");

		QueryBuilder query = QueryBuilders.termQuery("queueName.keyword", queueName);
		List<PollData> pollData = findAll(indexes.get(POLL_DATA), types.get(POLL_DATA), query, PollData.class);

		if (logger.isDebugEnabled())
			logger.debug("getPollData: result={}", toJson(pollData));
		return pollData;
	}

	@Override
	public List<Task> getPendingTasksByTags(String taskType, Set<String> tags) {
		QueryBuilder query = QueryBuilders.termsQuery("tags.keyword", tags);
		List<HashMap> wraps = findAll(indexes.get(WORKFLOW_TAGS), types.get(WORKFLOW_TAGS), query, HashMap.class);

		TermQueryBuilder statusTerm = QueryBuilders.termQuery("taskStatus", "IN_PROGRESS");
		TermQueryBuilder typeTerm = QueryBuilders.termQuery("taskType", taskType);

		String esIndexName = indexes.get(TASK);
		String esTypeName = types.get(TASK);

		return wraps.stream()
			.map(map -> {
				String workflowId = (String) map.get("workflowId");

				BoolQueryBuilder subQuery = QueryBuilders.boolQuery()
					.must(QueryBuilders.termQuery("workflowInstanceId", workflowId))
					.must(statusTerm)
					.must(typeTerm);

				return findAll(esIndexName, esTypeName, subQuery, Task.class);
			})
			.filter(Objects::nonNull)
			.flatMap(tasks -> tasks.stream().filter(Objects::nonNull))
			.collect(Collectors.toList());
	}

	@Override
	public boolean anyRunningWorkflowsByTags(Set<String> tags) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		tags.forEach(tag -> query.must(QueryBuilders.termQuery("tags.keyword", tag)));

		Long count = getCount(indexes.get(WORKFLOW_TAGS), types.get(WORKFLOW_TAGS), query);
		return count > 0;
	}

	@Override
	public void addEventPublished(EventPublished ep) {
		if (logger.isDebugEnabled())
			logger.debug("addEventPublished: ee={}", toJson(ep));
		try {
			String id = toId(ep.getSubject(), ep.getId());

			upsert(indexes.get(EVENT_PUBLISHED), types.get(EVENT_PUBLISHED), id, ep);

			if (logger.isDebugEnabled())
				logger.debug("addEventPublished: false");
		} catch (Exception ex) {
			if (logger.isDebugEnabled())
				logger.debug("addEventExecution: failed with {}", ex.getMessage());
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
		}
	}

	private List<String> dateStrBetweenDates(Long startdatems, Long enddatems) {
		if (logger.isDebugEnabled())
			logger.debug("dateStrBetweenDates: startdatems={}, enddatems={}", startdatems, enddatems);

		List<String> dates = Lists.newArrayList();
		Calendar calendar = new GregorianCalendar();
		Date startdate = new Date(startdatems);
		Date enddate = new Date(enddatems);
		calendar.setTime(startdate);
		while (calendar.getTime().before(enddate) || calendar.getTime().equals(enddate)) {
			Date result = calendar.getTime();
			dates.add(dateStr(result));
			calendar.add(Calendar.DATE, 1);
		}

		if (logger.isDebugEnabled())
			logger.debug("dateStrBetweenDates: result={}", dates);
		return dates;
	}

	private String insertOrUpdateWorkflow(Workflow workflow, boolean update) {
		Preconditions.checkNotNull(workflow, "workflow object cannot be null");

		if (workflow.getStatus().isTerminal()) {
			workflow.setEndTime(System.currentTimeMillis());
		}
		List<Task> tasks = workflow.getTasks();
		workflow.setTasks(Lists.newLinkedList());

		if (update) {
			updateWorkflowInternal(workflow);
		} else {
			addWorkflowInternal(workflow);
			addWorkflowDefToWorkflowMapping(workflow);
			if (StringUtils.isNotEmpty(workflow.getCorrelationId())) {
				addWorkflowToCorrIdMapping(workflow);
			}
		}

		// Add or remove from the pending workflows
		if (workflow.getStatus().isTerminal()) {
			deletePendingWorkflow(workflow);

			// We must not delete tags for RESET as it must be restarted right away
			if (workflow.getStatus() != Workflow.WorkflowStatus.RESET) {
				addOrDeleteWorkflowTags(workflow, false);
			}
		} else {
			addPendingWorkflow(workflow);
			addOrDeleteWorkflowTags(workflow, true);
		}

		workflow.setTasks(tasks);
		indexer.index(workflow);

		return workflow.getWorkflowId();
	}

	private void insertOrUpdateTask(Task task) {
		String indexName = indexes.get(TASK);
		String typeName = types.get(TASK);
		String id = toId(task.getTaskId());

		upsert(indexName, typeName, id, task);
	}

	private void deleteTask(Task task) {
		String indexName = indexes.get(TASK);
		String typeName = types.get(TASK);
		String id = toId(task.getTaskId());
		delete(indexName, typeName, id);
	}

	private boolean addScheduledTask(Task task) {
		String indexName = indexes.get(SCHEDULED_TASKS);
		String typeName = types.get(SCHEDULED_TASKS);
		String taskKey = task.getReferenceTaskName() + "" + task.getRetryCount();
		String id = toId(task.getWorkflowInstanceId(), taskKey); // Do not add taskId here!!!

		Map<String, Object> payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(),
			"taskRefName", task.getReferenceTaskName(),
			"taskId", task.getTaskId());

		return insert(indexName, typeName, id, payload);
	}

	private void deleteScheduledTask(Task task) {
		String indexName = indexes.get(SCHEDULED_TASKS);
		String typeName = types.get(SCHEDULED_TASKS);
		String taskKey = task.getReferenceTaskName() + String.valueOf(task.getRetryCount());
		String id = toId(task.getWorkflowInstanceId(), taskKey);
		delete(indexName, typeName, id);
	}

	private void addTaskToWorkflowMapping(Task task) {
		String indexName = indexes.get(WORKFLOW_TO_TASKS);
		String typeName = types.get(WORKFLOW_TO_TASKS);
		String id = toId(task.getWorkflowInstanceId(), task.getTaskId());

		Map<String, Object> payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(),
			"taskId", task.getTaskId());
		insert(indexName, typeName, id, payload);
	}

	private void deleteTaskToWorkflowMapping(Task task) {
		String indexName = indexes.get(WORKFLOW_TO_TASKS);
		String typeName = types.get(WORKFLOW_TO_TASKS);
		String id = toId(task.getWorkflowInstanceId(), task.getTaskId());
		delete(indexName, typeName, id);
	}

	private void addTaskInProgress(Task task) {
		String indexName = indexes.get(IN_PROGRESS_TASKS);
		String typeName = types.get(IN_PROGRESS_TASKS);
		String id = toId(task.getTaskDefName(), task.getTaskId());

		Map<String, Object> payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(),
			"taskDefName", task.getTaskDefName(),
			"taskId", task.getTaskId());
		insert(indexName, typeName, id, payload);
	}

	private void deleteTaskInProgress(Task task) {
		String indexName = indexes.get(IN_PROGRESS_TASKS);
		String typeName = types.get(IN_PROGRESS_TASKS);
		String id = toId(task.getTaskDefName(), task.getTaskId());
		delete(indexName, typeName, id);
	}

	private void addWorkflowInternal(Workflow workflow) {
		String indexName = indexes.get(WORKFLOW);
		String typeName = types.get(WORKFLOW);
		String id = toId(workflow.getWorkflowId());
		insert(indexName, typeName, id, workflow);
	}

	private void updateWorkflowInternal(Workflow workflow) {
		String indexName = indexes.get(WORKFLOW);
		String typeName = types.get(WORKFLOW);
		String id = toId(workflow.getWorkflowId());
		update(indexName, typeName, id, workflow);
	}

	private void deleteWorkflow(Workflow workflow) {
		String indexName = indexes.get(WORKFLOW);
		String typeName = types.get(WORKFLOW);
		String id = toId(workflow.getWorkflowId());
		delete(indexName, typeName, id);
	}

	private void addWorkflowDefToWorkflowMapping(Workflow workflow) {
		String indexName = indexes.get(WORKFLOW_DEF_TO_WORKFLOWS);
		String typeName = types.get(WORKFLOW_DEF_TO_WORKFLOWS);
		String id = toId(workflow.getWorkflowType(), dateStr(workflow.getCreateTime()), workflow.getWorkflowId());

		Map<String, Object> payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
			"workflowType", workflow.getWorkflowType(),
			"dateStr", dateStr(workflow.getCreateTime()));
		insert(indexName, typeName, id, payload);
	}

	private void deleteWorkflowDefToWorkflowMapping(Workflow workflow) {
		String indexName = indexes.get(WORKFLOW_DEF_TO_WORKFLOWS);
		String typeName = types.get(WORKFLOW_DEF_TO_WORKFLOWS);
		String id = toId(workflow.getWorkflowType(), dateStr(workflow.getCreateTime()), workflow.getWorkflowId());
		delete(indexName, typeName, id);
	}

	private void addWorkflowToCorrIdMapping(Workflow workflow) {
		if (StringUtils.isEmpty(workflow.getCorrelationId())) {
			return;
		}
		String sha256hex = DigestUtils.sha256Hex(workflow.getCorrelationId());
		String indexName = indexes.get(CORR_ID_TO_WORKFLOWS);
		String typeName = types.get(CORR_ID_TO_WORKFLOWS);
		String id = toId(sha256hex, workflow.getWorkflowId());

		Map<String, Object> payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
			"correlationId", workflow.getCorrelationId(),
			"sha256hex", sha256hex);
		insert(indexName, typeName, id, payload);
	}

	private void deleteWorkflowToCorrIdMapping(Workflow workflow) {
		if (StringUtils.isEmpty(workflow.getCorrelationId())) {
			return;
		}
		String sha256hex = DigestUtils.sha256Hex(workflow.getCorrelationId());
		String indexName = indexes.get(CORR_ID_TO_WORKFLOWS);
		String typeName = types.get(CORR_ID_TO_WORKFLOWS);
		String id = toId(sha256hex, workflow.getWorkflowId());
		delete(indexName, typeName, id);
	}

	private void addPendingWorkflow(Workflow workflow) {
		String indexName = indexes.get(PENDING_WORKFLOWS);
		String typeName = types.get(PENDING_WORKFLOWS);
		String id = toId(workflow.getWorkflowType(), workflow.getWorkflowId());

		Map<String, Object> payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
			"workflowType", workflow.getWorkflowType());
		insert(indexName, typeName, id, payload);
	}

	private void addOrDeleteWorkflowTags(Workflow workflow, boolean add) {
		String indexName = indexes.get(WORKFLOW_TAGS);
		String typeName = types.get(WORKFLOW_TAGS);
		String id = toId(workflow.getWorkflowId());

		if (add) {
			if (CollectionUtils.isEmpty(workflow.getTags())) {
				return;
			}

			Map<String, Object> payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
				"tags", workflow.getTags());

			insert(indexName, typeName, id, payload);
		} else {
			delete(indexName, typeName, id);
		}
	}

	private void deletePendingWorkflow(String workflowType, String workflowId) {
		String indexName = indexes.get(PENDING_WORKFLOWS);
		String typeName = types.get(PENDING_WORKFLOWS);
		String id = toId(workflowType, workflowId);
		delete(indexName, typeName, id);
	}

	private void deletePendingWorkflow(Workflow workflow) {
		deletePendingWorkflow(workflow.getWorkflowType(), workflow.getWorkflowId());
	}

	private String dateStr(Long timeInMs) {
		Date date = new Date(timeInMs);
		return dateStr(date);
	}

	private String dateStr(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		return format.format(date);
	}

	public void addErrorRegistry(WorkflowErrorRegistry workflowErrorRegistry) {

	}

	public List<WorkflowError> searchWorkflowErrorRegistry(WorkflowErrorRegistry workflowErrorRegistry) {
		return null;
	}

	public List<WorkflowErrorRegistry> searchWorkflowErrorRegistryList(WorkflowErrorRegistry workflowErrorRegistry) {
		return null;
	}

	public List<TaskDetails> searchTaskDetails(String jobId, String workflowId, String workflowType, String taskName, Boolean includeOutput) {
		return null;
	}
}

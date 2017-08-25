package com.netflix.conductor.dao.mysql;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.join;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;

class MySQLExecutionDAO extends MySQLBaseDAO implements ExecutionDAO {

	private static final String ARCHIVED_FIELD = "archived";
	private static final String RAW_JSON_FIELD = "rawJSON";

	private static final String INSERT_SCHEDULED_TASK = "INSERT INTO task_scheduled (workflow_id, task_key, task_id) VALUES (:workflowId, :taskKey, :taskId)";
	private static final String REMOVE_SCHEDULED_TASK = "DELETE FROM task_scheduled WHERE workflow_id = :workflowId AND task_key = :taskKey";
	private static final String EXISTS_SCHEDULED_TASK = "SELECT EXISTS(SELECT 1 FROM task_scheduled WHERE workflow_id = :workflowId AND task_key = :taskKey)";

	private static final String INSERT_WORKFLOW_TO_TASK = "INSERT INTO workflow_to_task (workflow_id, task_id) VALUES (:workflowId, :taskId)";
	private static final String EXISTS_WORKFLOW_TO_TASK = "SELECT EXISTS(SELECT 1 FROM workflow_to_task WHERE workflow_id = :workflowId AND task_id = :taskId)";
	private static final String REMOVE_WORKFLOW_TO_TASK = "DELETE FROM workflow_to_task WHERE workflow_id = :workflowId AND task_id = :taskId";
	private static final String GET_TASKS_FOR_WORKFLOW = "SELECT task_id FROM workflow_to_task WHERE workflow_id = :workflowId";

	private static final String INSERT_IN_PROGRESS_TASK = "INSERT INTO task_in_progress (task_def_name, task_id) VALUES (:taskDefName, :taskId)";
	private static final String REMOVE_IN_PROGRESS_TASK = "DELETE FROM task_in_progress WHERE task_def_name = :taskDefName AND task_id = :taskId";
	private static final String EXISTS_TASK_IN_PROGRESS = "SELECT EXISTS(SELECT 1 FROM task_in_progress WHERE task_def_name = :taskDefName AND task_id = :taskId)";
	private static final String UPDATE_IN_PROGRESS_TASK_STATUS = "UPDATE task_in_progress SET in_progress_status = :inProgress, modified_on = CURRENT_TIMESTAMP WHERE task_def_name = :taskDefName AND task_id = :taskId";
	private static final String GET_ALL_IN_PROGRESS_FOR_TASK_TYPE = "SELECT task_id FROM task_in_progress WHERE task_def_name = :taskDefName";

	private static final String GET_IN_PROGRESS_TASK_COUNT = "SELECT COUNT(*) FROM task_in_progress WHERE task_def_name = :taskDefName AND in_progress_status = true";
	private static final String GET_IN_PROGRESS_TASKS_WITH_LIMIT = "SELECT task_id FROM task_in_progress WHERE task_def_name = :taskDefName ORDER BY id LIMIT :limit";

	private static final String INSERT_TASK = "INSERT INTO task (task_id, json_data) VALUES (:taskId, :jsonData)";
	private static final String UPDATE_TASK = "UPDATE task SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP WHERE task_id = :taskId";
	private static final String REMOVE_TASK = "DELETE FROM task WHERE task_id = :taskId";
	private static final String GET_TASKS = "SELECT json_data FROM task WHERE task_id IN (%s)";
	private static final String GET_TASK = "SELECT json_data FROM task WHERE task_id = :taskId";

	private static final String INSERT_WORKFLOW = "INSERT INTO workflow (workflow_id, correlation_id, json_data) VALUES (:workflowId, :correlationId, :jsonData)";
	private static final String UPDATE_WORKFLOW = "UPDATE workflow SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP WHERE workflow_id = :workflowId";
	private static final String REMOVE_WORKFLOW = "DELETE FROM workflow WHERE workflow_id = :workflowId";
	private static final String GET_WORKFLOW = "SELECT json_data FROM workflow WHERE workflow_id = :workflowId";
	private static final String GET_WORKFLOWS_BY_CORRELATION_ID = "SELECT workflow_id FROM workflow WHERE correlation_id = :correlationId";

	private static final String INSERT_WORKFLOW_DEF_TO_WORKFLOW = "INSERT INTO workflow_def_to_workflow (workflow_def, date_str, workflow_id) VALUES (:workflowType, :dateStr, :workflowId)";
	private static final String REMOVE_WORKFLOW_DEF_TO_WORKFLOW = "DELETE FROM workflow_def_to_workflow WHERE workflow_def = :workflowType AND date_str = :dateStr AND workflow_id = :workflowId";
	private static final String GET_ALL_WORKFLOWS_FOR_WORKFLOW_DEF = "SELECT workflow_id FROM workflow_def_to_workflow WHERE workflow_def = :workflowType AND date_str BETWEEN :start AND :end";

	private static final String INSERT_PENDING_WORKFLOW = "INSERT INTO workflow_pending (workflow_type, workflow_id) VALUES (:workflowType, :workflowId)";
	private static final String EXISTS_PENDING_WORKFLOW = "SELECT EXISTS(SELECT 1 FROM workflow_pending WHERE workflow_type = :workflowType AND workflow_id = :workflowId)";
	private static final String REMOVE_PENDING_WORKFLOW = "DELETE FROM workflow_pending WHERE workflow_type = :workflowType AND workflow_id = :workflowId";
	private static final String GET_PENDING_WORKFLOW_IDS = "SELECT workflow_id FROM workflow_pending WHERE workflow_type = :workflowType";
	private static final String GET_PENDING_WORKFLOW_COUNT = "SELECT COUNT(*) FROM workflow_pending WHERE workflow_type = :workflowType";

	private static final String INSERT_EVENT_EXECUTION = "INSERT INTO event_execution (event_handler_name, event_name, message_id, execution_id, json_data) VALUES (:name, :event, :messageId, :id, :jsonData)";
	private static final String UPDATE_EVENT_EXECUTION = "UPDATE event_execution SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP WHERE event_handler_name = :name AND execution_event = :event AND message_id = :messageId AND execution_id = :id";
	private static final String EXISTS_EVENT_EXECUTION = "SELECT EXISTS(SELECT 1 FROM event_execution WHERE event_handler_name = :name AND event_name = :event AND message_id = :messageId AND execution_id = :id)";
	private static final String GET_EVENT_EXECUTION = "SELECT json_data FROM event_execution WHERE event_handler_name = :name AND event_name = :event AND message_id = :messageId AND execution_id = :id";

	private static final String INSERT_POLL_DATA = "INSERT INTO poll_data (queue_name, domain, json_data) VALUES (:queueName, :domain, :jsonData)";
	private static final String UPDATE_POLL_DATA = "UPDATE poll_data SET json_data = :jsonData, modified_on = CURRENT_TIMESTAMP WHERE queue_name = :queueName AND domain = :domain";
	private static final String GET_POLL_DATA = "SELECT json_data FROM poll_data WHERE queue_name = :queueName AND domain = :domain";
	private static final String GET_ALL_POLL_DATA = "SELECT json_data FROM poll_data WHERE queue_name = :queueName";

	private IndexDAO indexer;

	private MetadataDAO metadata;

	@Inject
	MySQLExecutionDAO(IndexDAO indexer, MetadataDAO metadata, ObjectMapper om, Sql2o sql2o) {
		super(om, sql2o);
		this.indexer = indexer;
		this.metadata = metadata;
	}

	@Override
	public List<Task> getPendingTasksByWorkflow(String taskDefName, String workflowId) {
		return getPendingTasksForTaskType(taskDefName).stream()
				.filter(task -> task.getWorkflowInstanceId().equals(workflowId))
				.collect(Collectors.toList());
	}

	@Override
	public List<Task> getTasks(String taskDefName, String startKey, int count) {
		List<Task> tasks = new LinkedList<>();

		List<Task> pendingTasks = getPendingTasksForTaskType(taskDefName);
		boolean startKeyFound = startKey == null;
		int found = 0;
		for (Task pendingTask : pendingTasks) {
			if (!startKeyFound) {
				if (pendingTask.getTaskId().equals(startKey)) {
					startKeyFound = true;
					if (startKey != null) {
						continue;
					}
				}
			}
			if (startKeyFound && found < count) {
				tasks.add(pendingTask);
				found++;
			}
		}

		return tasks;
	}

	@Override
	public List<Task> createTasks(List<Task> tasks) {
		List<Task> created = Lists.newLinkedList();

		withTransaction(connection -> {
			for (Task task : tasks) {
				validate(task);

				task.setScheduledTime(System.currentTimeMillis());

				String taskKey = task.getReferenceTaskName() + "_" + task.getRetryCount();

				boolean scheduledTaskAdded = addScheduledTask(connection, task, taskKey);

				if (!scheduledTaskAdded) {
					logger.info("Task already scheduled, skipping the run " + task.getTaskId() + ", ref=" + task.getReferenceTaskName() + ", key=" + taskKey);
					continue;
				}

				insertOrUpdateTaskData(connection, task, INSERT_TASK);
				addWorkflowToTaskMapping(connection, task);
				addTaskInProgress(connection, task);
				updateTask(connection, task);

				created.add(task);
			}
		});

		return created;
	}

	@Override
	public void updateTask(Task task) {
		withTransaction(connection -> updateTask(connection, task));
	}

	@Override
	public boolean exceedsInProgressLimit(Task task) {
		TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());
		if (taskDef == null) return false;

		int limit = taskDef.concurrencyLimit();
		if (limit <= 0) return false;

		long current = getInProgressTaskCount(task.getTaskDefName());

		if (current >= limit) {
			Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
			return true;
		}

		logger.info("Task execution count for {}: limit={}, current={}", task.getTaskDefName(), limit, getInProgressTaskCount(task.getTaskDefName()));

		String taskId = task.getTaskId();

		List<String> tasksInProgressInOrderOfArrival = findAllTasksInProgressInOrderOfArrival(task, limit);

		boolean rateLimited = !tasksInProgressInOrderOfArrival.contains(taskId);

		if (rateLimited) {
			logger.info("Task execution count limited. {}, limit {}, current {}", task.getTaskDefName(), limit, getInProgressTaskCount(task.getTaskDefName()));
			Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
		}

		return rateLimited;
	}

	@Override
	public void updateTasks(List<Task> tasks) {
		withTransaction(connection -> tasks.forEach(task -> updateTask(connection, task)));
	}

	@Override
	public void addTaskExecLog(List<TaskExecLog> log) {
		indexer.add(log);
	}

	@Override
	public void removeTask(String taskId) {
		Task task = getTask(taskId);

		if(task == null) {
			logger.warn("No such Task by id {}", taskId);
			return;
		}

		String taskKey = task.getReferenceTaskName() + "" + task.getRetryCount();

		withTransaction(connection -> {
			removeScheduledTask(connection, task, taskKey);
			removeWorkflowToTaskMapping(connection, task);
			removeTaskInProgress(connection, task);
			removeTaskData(connection, task);
		});
	}

	@Override
	public Task getTask(String taskId) {
		String taskJsonStr = getWithTransaction(c -> c.createQuery(GET_TASK).addParameter("taskId", taskId).executeScalar(String.class));
		return taskJsonStr != null ? readValue(taskJsonStr, Task.class) : null;
	}

	@Override
	public List<Task> getTasks(List<String> taskIds) {
		if (taskIds.isEmpty()) return Lists.newArrayList();

		List<String> taskIdParameters = taskIds.stream().map(taskId -> format("'%s'", taskId)).collect(Collectors.toList());

		return getWithTransaction(c -> c.createQuery(format(GET_TASKS, join(taskIdParameters, ","))).executeScalarList(String.class))
				.stream()
				.filter(Objects::nonNull)
				.map(taskJsonStr -> readValue(taskJsonStr, Task.class))
				.collect(Collectors.toList());
	}

	@Override
	public List<Task> getPendingTasksForTaskType(String taskName) {
		Preconditions.checkNotNull(taskName, "task name cannot be null");

		List<String> taskIds = getWithTransaction(connection -> connection.createQuery(GET_ALL_IN_PROGRESS_FOR_TASK_TYPE)
				.addParameter("taskDefName", taskName)
				.executeScalarList(String.class));

		return getTasks(taskIds);
	}

	@Override
	public List<Task> getTasksForWorkflow(String workflowId) {
		List<String> taskIds = getWithTransaction(c -> c.createQuery(GET_TASKS_FOR_WORKFLOW)
				.addParameter("workflowId", workflowId)
				.executeScalarList(String.class));

		return getTasks(taskIds);
	}

	@Override
	public String createWorkflow(Workflow workflow) {
		workflow.setCreateTime(System.currentTimeMillis());
		return insertOrUpdateWorkflow(workflow, false);
	}

	@Override
	public String updateWorkflow(Workflow workflow) {
		workflow.setUpdateTime(System.currentTimeMillis());
		return insertOrUpdateWorkflow(workflow, true);
	}

	@Override
	public void removeWorkflow(String workflowId) {
		try {
			Workflow wf = getWorkflow(workflowId, true);

			//Add to elasticsearch
			indexer.update(workflowId, new String[]{RAW_JSON_FIELD, ARCHIVED_FIELD}, new Object[]{om.writeValueAsString(wf), true});

			withTransaction(connection -> {
				removeWorkflowDefToWorkflowMapping(connection, wf);
				removeWorkflow(connection, workflowId);
				removePendingWorkflow(connection, wf.getWorkflowType(), workflowId);
			});

			for(Task task : wf.getTasks()) {
				removeTask(task.getTaskId());
			}

		} catch(Exception e) {
			throw new ApplicationException(e.getMessage(), e);
		}
	}

	@Override
	public void removeFromPendingWorkflow(String workflowType, String workflowId) {
		withTransaction(connection -> removePendingWorkflow(connection, workflowType, workflowId));
	}

	@Override
	public Workflow getWorkflow(String workflowId) {
		return getWorkflow(workflowId, true);
	}

	@Override
	public Workflow getWorkflow(String workflowId, boolean includeTasks) {
		Workflow workflow = getWithTransaction(tx -> readWorkflow(tx, workflowId));

		if (workflow != null) {
			if (includeTasks) {
				List<Task> tasks = getTasksForWorkflow(workflowId);
				tasks.sort(Comparator.comparingLong(Task::getScheduledTime).thenComparingInt(Task::getSeq));
				workflow.setTasks(tasks);
			}
			return workflow;
		}

		//try from the archive
		workflow = readWorkflowFromArchive(workflowId);

		if(!includeTasks) {
			workflow.getTasks().clear();
		}

		return workflow;
	}

	@Override
	public List<String> getRunningWorkflowIds(String workflowName) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		return getWithTransaction(tx -> tx.createQuery(GET_PENDING_WORKFLOW_IDS).addParameter("workflowType", workflowName).executeScalarList(String.class));
	}

	@Override
	public List<Workflow> getPendingWorkflowsByType(String workflowName) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		return getRunningWorkflowIds(workflowName).stream().map(this::getWorkflow).collect(Collectors.toList());
	}

	@Override
	public long getPendingWorkflowCount(String workflowName) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		return getWithTransaction(tx -> tx.createQuery(GET_PENDING_WORKFLOW_COUNT).addParameter("workflowType", workflowName).executeScalar(Long.class));
	}

	@Override
	public long getInProgressTaskCount(String taskDefName) {
		return getWithTransaction(c -> c.createQuery(GET_IN_PROGRESS_TASK_COUNT)
				.addParameter("taskDefName", taskDefName)
				.executeScalar(Long.class));
	}

	@Override
	public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		Preconditions.checkNotNull(startTime, "startTime cannot be null");
		Preconditions.checkNotNull(endTime, "endTime cannot be null");

		List<Workflow> workflows = new LinkedList<Workflow>();

		withTransaction(tx -> {
			List<String> workflowIds = tx.createQuery(GET_ALL_WORKFLOWS_FOR_WORKFLOW_DEF)
					.addParameter("workflowType", workflowName)
					.addParameter("start", dateStr(startTime))
					.addParameter("end", dateStr(endTime))
					.executeScalarList(String.class);

			workflowIds.forEach(workflowId -> {
				try {
					Workflow wf = getWorkflow(workflowId);
					if (wf.getCreateTime() >= startTime && wf.getCreateTime() <= endTime) {
						workflows.add(wf);
					}
				} catch(Exception e) {
					logger.error(e.getMessage(), e);
				}
			});
		});

		return workflows;
	}

	@Override
	public List<Workflow> getWorkflowsByCorrelationId(String correlationId) {
		Preconditions.checkNotNull(correlationId, "correlationId cannot be null");
		return getWithTransaction(tx -> tx.createQuery(GET_WORKFLOWS_BY_CORRELATION_ID)
				.addParameter("correlationId", correlationId)
				.executeScalarList(String.class)).stream()
				.map(this::getWorkflow)
				.collect(Collectors.toList());
	}

	@Override
	public boolean addEventExecution(EventExecution eventExecution) {
		try {
			boolean added = getWithTransaction(tx -> insertEventExecution(tx, eventExecution));
			if (added) {
				indexer.add(eventExecution);
				return true;
			}
			return false;
		} catch (Exception e) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public void updateEventExecution(EventExecution eventExecution) {
		try {
			withTransaction(tx -> updateEventExecution(tx, eventExecution));
			indexer.add(eventExecution);
		} catch (Exception e) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max) {
		try {
			List<EventExecution> executions = Lists.newLinkedList();
			withTransaction(tx ->  {
				for(int i = 0; i < max; i++) {
					String executionId = messageId + "_" + i; //see EventProcessor.handle to understand how the execution id is set
					EventExecution ee = readEventExecution(tx, eventHandlerName, eventName, messageId, executionId);
					if (ee == null) break;
					executions.add(ee);
				}
			});
			return executions;
		} catch (Exception e) {
			throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public void addMessage(String queue, Message msg) {
		indexer.addMessage(queue, msg);
	}

	@Override
	public void updateLastPoll(String taskDefName, String domain, String workerId) {
		Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
		PollData pollData = new PollData(taskDefName, domain, workerId, System.currentTimeMillis());

		String effectiveDomain = (domain == null) ? "DEFAULT" : domain;

		withTransaction(tx -> {
			int updated = insertOrUpdatePollData(tx, UPDATE_POLL_DATA, pollData, effectiveDomain);
			if (updated == 0) {
				insertOrUpdatePollData(tx, INSERT_POLL_DATA, pollData, effectiveDomain);
			}
		});
	}

	@Override
	public PollData getPollData(String taskDefName, String domain) {
		Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
		String effectiveDomain = (domain == null) ? "DEFAULT" : domain;
		return getWithTransaction(tx -> readPollData(tx, taskDefName, effectiveDomain));
	}

	@Override
	public List<PollData> getPollData(String taskDefName) {
		Preconditions.checkNotNull(taskDefName, "taskDefName name cannot be null");
		return readAllPollData(taskDefName);
	}

	private String insertOrUpdateWorkflow(Workflow workflow, boolean update) {
		Preconditions.checkNotNull(workflow, "workflow object cannot be null");

		boolean terminal = workflow.getStatus().isTerminal();

		if (terminal) workflow.setEndTime(System.currentTimeMillis());

		List<Task> tasks = workflow.getTasks();
		workflow.setTasks(Lists.newLinkedList());

		withTransaction(tx -> {
			if (!update) {
				addWorkflow(tx, workflow);
				addWorkflowDefToWorkflowMapping(tx, workflow);
			} else {
				updateWorkflow(tx, workflow);
			}

			if (terminal) {
				removePendingWorkflow(tx, workflow.getWorkflowType(), workflow.getWorkflowId());
			} else {
				addPendingWorkflow(tx, workflow.getWorkflowType(), workflow.getWorkflowId());
			}
		});

		workflow.setTasks(tasks);
		indexer.index(workflow);
		return workflow.getWorkflowId();
	}

	private void updateTask(Connection connection, Task task) {
		task.setUpdateTime(System.currentTimeMillis());
		if (task.getStatus() != null && task.getStatus().isTerminal()) {
			task.setEndTime(System.currentTimeMillis());
		}

		TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());

		if (taskDef != null && taskDef.concurrencyLimit() > 0) {
			boolean inProgress = task.getStatus() != null && task.getStatus().equals(Task.Status.IN_PROGRESS);
			updateInProgressStatus(connection, task, inProgress);
		}

		insertOrUpdateTaskData(connection, task, UPDATE_TASK);

		if (task.getStatus() != null && task.getStatus().isTerminal()) {
			removeTaskInProgress(connection, task);
		}

		indexer.index(task);
	}

	private Workflow readWorkflow(Connection connection, String workflowId) {
		String json = connection.createQuery(GET_WORKFLOW).addParameter("workflowId", workflowId).executeScalar(String.class);
		return json != null ? readValue(json, Workflow.class) : null;
	}

	private Workflow readWorkflowFromArchive(String workflowId) {
		String json = indexer.get(workflowId, RAW_JSON_FIELD);
		if (json != null) {
			return readValue(json, Workflow.class);
		} else {
			throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "No such workflow found by id: " + workflowId);
		}
	}

	private void addWorkflow(Connection connection, Workflow workflow) {
		connection.createQuery(INSERT_WORKFLOW)
				.addParameter("workflowId", workflow.getWorkflowId())
				.addParameter("correlationId", workflow.getCorrelationId())
				.addParameter("jsonData", toJson(workflow))
				.executeUpdate();
	}

	private void updateWorkflow(Connection connection, Workflow workflow) {
		connection.createQuery(UPDATE_WORKFLOW)
				.addParameter("workflowId", workflow.getWorkflowId())
				.addParameter("jsonData", toJson(workflow))
				.executeUpdate();
	}

	private void removeWorkflow(Connection connection, String workflowId) {
		connection.createQuery(REMOVE_WORKFLOW)
				.addParameter("workflowId", workflowId)
				.executeUpdate();
	}

	private void addPendingWorkflow(Connection connection, String workflowType, String workflowId) {
		boolean exist = connection.createQuery(EXISTS_PENDING_WORKFLOW)
				.addParameter("workflowType", workflowType)
				.addParameter("workflowId", workflowId)
				.executeScalar(Boolean.class);

		if (!exist) {
			connection.createQuery(INSERT_PENDING_WORKFLOW)
					.addParameter("workflowType", workflowType)
					.addParameter("workflowId", workflowId)
					.executeUpdate();
		}
	}

	private void removePendingWorkflow(Connection connection, String workflowType, String workflowId) {
		connection.createQuery(REMOVE_PENDING_WORKFLOW)
				.addParameter("workflowType", workflowType)
				.addParameter("workflowId", workflowId)
				.executeUpdate();
	}

	private void insertOrUpdateTaskData(Connection connection, Task task, String query) {
		int result = connection.createQuery(UPDATE_TASK).addParameter("taskId", task.getTaskId()).addParameter("jsonData", toJson(task)).executeUpdate().getResult();
		if (result == 0) {
			connection.createQuery(INSERT_TASK).addParameter("taskId", task.getTaskId()).addParameter("jsonData", toJson(task)).executeUpdate().getResult();
		}
	}

	private void removeTaskData(Connection connection, Task task) {
		connection.createQuery(REMOVE_TASK)
				.addParameter("taskId", task.getTaskId())
				.executeUpdate();
	}

	private void addWorkflowToTaskMapping(Connection connection, Task task) {
		boolean exist = connection.createQuery(EXISTS_WORKFLOW_TO_TASK)
				.addParameter("workflowId", task.getWorkflowInstanceId())
				.addParameter("taskId", task.getTaskId())
				.executeScalar(Boolean.class);

		if (!exist) {
			connection.createQuery(INSERT_WORKFLOW_TO_TASK)
					.addParameter("workflowId", task.getWorkflowInstanceId())
					.addParameter("taskId", task.getTaskId())
					.executeUpdate();
		}
	}

	private void removeWorkflowToTaskMapping(Connection connection, Task task) {
		connection.createQuery(REMOVE_WORKFLOW_TO_TASK)
				.addParameter("workflowId", task.getWorkflowInstanceId())
				.addParameter("taskId", task.getTaskId())
				.executeUpdate();
	}

	private void addWorkflowDefToWorkflowMapping(Connection connection, Workflow workflow) {
		connection.createQuery(INSERT_WORKFLOW_DEF_TO_WORKFLOW)
				.bind(workflow)
				.addParameter("dateStr", dateStr(workflow.getCreateTime()))
				.executeUpdate();
	}

	private void removeWorkflowDefToWorkflowMapping(Connection connection, Workflow workflow) {
		connection.createQuery(REMOVE_WORKFLOW_DEF_TO_WORKFLOW)
				.bind(workflow)
				.addParameter("dateStr", dateStr(workflow.getCreateTime()))
				.executeUpdate();
	}

	private boolean addScheduledTask(Connection connection, Task task, String taskKey) {
		boolean exist = connection.createQuery(EXISTS_SCHEDULED_TASK)
				.addParameter("workflowId", task.getWorkflowInstanceId())
				.addParameter("taskKey", taskKey)
				.executeScalar(Boolean.class);

		if (!exist) {
			connection.createQuery(INSERT_SCHEDULED_TASK)
					.addParameter("workflowId", task.getWorkflowInstanceId())
					.addParameter("taskKey", taskKey)
					.addParameter("taskId", task.getTaskId())
					.executeUpdate()
					.getResult();
			return true;
		}

		return false;
	}

	private void removeScheduledTask(Connection connection, Task task, String taskKey) {
		connection.createQuery(REMOVE_SCHEDULED_TASK)
				.addParameter("workflowId", task.getWorkflowInstanceId())
				.addParameter("taskKey", taskKey)
				.executeUpdate()
				.getResult();
	}

	private void addTaskInProgress(Connection connection, Task task) {
		boolean exist = connection.createQuery(EXISTS_TASK_IN_PROGRESS)
				.addParameter("taskDefName", task.getTaskDefName())
				.addParameter("taskId", task.getTaskId())
				.executeScalar(Boolean.class);

		if (!exist) {
			connection.createQuery(INSERT_IN_PROGRESS_TASK)
					.addParameter("taskDefName", task.getTaskDefName())
					.addParameter("taskId", task.getTaskId())
					.executeUpdate();
		}
	}

	private void removeTaskInProgress(Connection connection, Task task) {
		connection.createQuery(REMOVE_IN_PROGRESS_TASK)
				.addParameter("taskDefName", task.getTaskDefName())
				.addParameter("taskId", task.getTaskId())
				.executeUpdate();
	}

	private void updateInProgressStatus(Connection connection, Task task, boolean inProgress) {
		connection.createQuery(UPDATE_IN_PROGRESS_TASK_STATUS)
				.addParameter("taskDefName", task.getTaskDefName())
				.addParameter("taskId", task.getTaskId())
				.addParameter("inProgress", inProgress)
				.executeUpdate();
	}

	private boolean insertEventExecution(Connection  connection, EventExecution eventExecution) {
		boolean exist = connection.createQuery(EXISTS_EVENT_EXECUTION).bind(eventExecution).executeScalar(Boolean.class);
		if (!exist) {
			connection.createQuery(INSERT_EVENT_EXECUTION).bind(eventExecution).addParameter("jsonData", toJson(eventExecution)).executeUpdate();
			return true;
		}
		return false;
	}

	private void updateEventExecution(Connection  connection, EventExecution eventExecution) {
		connection.createQuery(UPDATE_EVENT_EXECUTION).bind(eventExecution).addParameter("jsonData", toJson(eventExecution)).executeUpdate();
	}

	private EventExecution readEventExecution(Connection connection, String eventHandlerName, String eventName, String messageId, String executionId) {
		String jsonStr = connection.createQuery(GET_EVENT_EXECUTION)
				.addParameter("name", eventHandlerName)
				.addParameter("event", eventName)
				.addParameter("messageId", messageId)
				.addParameter("id", executionId)
				.executeScalar(String.class);
		return jsonStr != null ? readValue(jsonStr, EventExecution.class) : null;
	}

	private int insertOrUpdatePollData(Connection connection, String query, PollData pollData, String domain) {
		return connection.createQuery(query)
				.addParameter("queueName", pollData.getQueueName())
				.addParameter("domain", domain)
				.addParameter("jsonData", toJson(pollData))
				.executeUpdate()
				.getResult();
	}

	private PollData readPollData(Connection connection, String queueName, String domain) {
		String jsonStr = connection.createQuery(GET_POLL_DATA)
				.addParameter("queueName", queueName)
				.addParameter("domain", domain)
				.executeScalar(String.class);
		return jsonStr != null ? readValue(jsonStr, PollData.class) : null;
	}

	private List<PollData> readAllPollData(String queueName) {
		return getWithTransaction(tx -> tx.createQuery(GET_ALL_POLL_DATA)
				.addParameter("queueName", queueName)
				.executeScalarList(String.class)
				.stream()
				.map(jsonData -> readValue(jsonData, PollData.class))
				.collect(Collectors.toList()));
	}

	private List<String> findAllTasksInProgressInOrderOfArrival(Task task, int limit) {
		return getWithTransaction(connection ->
			connection.createQuery(GET_IN_PROGRESS_TASKS_WITH_LIMIT)
					.addParameter("taskDefName", task.getTaskDefName())
					.addParameter("limit", limit)
					.executeScalarList(String.class));
	}

	private void validate(Task task) {
		Preconditions.checkNotNull(task, "task object cannot be null");
		Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
		Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
		Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");
	}

	private static String dateStr(Long timeInMs) {
		Date date = new Date(timeInMs);
		return dateStr(date);
	}

	private static String dateStr(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		return format.format(date);
	}
}

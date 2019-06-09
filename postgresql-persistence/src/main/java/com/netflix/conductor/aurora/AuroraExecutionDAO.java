package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.conductor.aurora.sql.Query;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventPublished;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class AuroraExecutionDAO extends AuroraBaseDAO implements ExecutionDAO {
	private static final Logger logger = LoggerFactory.getLogger(AuroraExecutionDAO.class);
	private static final String ARCHIVED_FIELD = "archived";
	private static final String RAW_JSON_FIELD = "rawJSON";
	private MetadataDAO metadata;
	private IndexDAO indexer;

	@Inject
	public AuroraExecutionDAO(DataSource dataSource, ObjectMapper mapper,
							  MetadataDAO metadata, IndexDAO indexer) {
		super(dataSource, mapper);
		this.metadata = metadata;
		this.indexer = indexer;
	}

	@Override
	public List<Task> getPendingTasksByWorkflow(String taskName, String workflowId) {
		String SQL = "SELECT t.json_data FROM task_in_progress tip " +
			"INNER JOIN task t ON t.task_id = tip.task_id " +
			"WHERE tip.task_def_name = ? AND tip.workflow_id = ?";

		return queryWithTransaction(SQL, q -> q.addParameter(taskName)
			.addParameter(workflowId)
			.executeAndFetch(Task.class));
	}

	@Override
	public List<Task> getPendingTasksForTaskType(String taskType) {
		String SQL = "SELECT t.json_data FROM task_in_progress tip " +
			"INNER JOIN task t ON t.task_id = tip.task_id " +
			"WHERE tip.task_def_name = ?";

		return queryWithTransaction(SQL,
			q -> q.addParameter(taskType).executeAndFetch(Task.class));
	}

	@Override
	public List<Task> getPendingSystemTasks(String taskType) {
		String SQL = "SELECT t.json_data FROM task_in_progress tip " +
			"INNER JOIN task t ON t.task_id = tip.task_id " +
			"WHERE t.task_type = ?";

		return queryWithTransaction(SQL,
			q -> q.addParameter(taskType).executeAndFetch(Task.class));
	}

	@Override
	public List<Task> getTasks(String taskType, String startKey, int count) {
		List<Task> tasks = Lists.newLinkedList();

		List<Task> pendingTasks = getPendingTasksForTaskType(taskType);
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
		List<Task> created = Lists.newLinkedList();

		withTransaction(connection -> {
			for (Task task : tasks) {

				Preconditions.checkNotNull(task, "task object cannot be null");
				Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
				Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
				Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");

				task.setScheduledTime(System.currentTimeMillis());

				boolean taskAdded = addScheduledTask(connection, task);
				if (!taskAdded) {
					String taskKey = task.getReferenceTaskName() + task.getRetryCount();
					if (logger.isDebugEnabled())
						logger.debug("Task already scheduled, skipping the run " + task.getTaskId() +
							", ref=" + task.getReferenceTaskName() + ", key=" + taskKey);
					continue;
				}
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
		if (taskDef == null) {
			return false;
		}

		int limit = taskDef.concurrencyLimit();
		if (limit <= 0) {
			return false;
		}

		long current = getInProgressTaskCount(task.getTaskDefName());
		if (current >= limit) {
			return true;
		}

		logger.info("Task execution count for {}: limit={}, current={}", task.getTaskDefName(), limit,
			getInProgressTaskCount(task.getTaskDefName()));

		String taskId = task.getTaskId();

		List<String> tasksInProgressInOrderOfArrival = findAllTasksInProgressInOrderOfArrival(task, limit);

		boolean rateLimited = !tasksInProgressInOrderOfArrival.contains(taskId);

		if (rateLimited) {
			logger.info("Task execution count limited. {}, limit {}, current {}", task.getTaskDefName(), limit,
				getInProgressTaskCount(task.getTaskDefName()));
		}

		return rateLimited;
	}

	@Override
	public void updateTasks(List<Task> tasks) {
		withTransaction(connection -> {
			for (Task task : tasks) {
				updateTask(connection, task);
			}
		});
	}

	@Override
	public void addTaskExecLog(List<TaskExecLog> log) {
		indexer.add(log);
	}

	@Override
	public void removeTask(String taskId) {
		Task task = getTask(taskId);

		if (task == null) {
			logger.warn("No such task found by id {}", taskId);
			return;
		}

		withTransaction(connection -> {
			removeTask(connection, task);
		});
	}

	@Override
	public Task getTask(String taskId) {
		String GET_TASK = "SELECT json_data FROM task WHERE task_id = ?";
		return queryWithTransaction(GET_TASK, q -> q.addParameter(taskId).executeAndFetchFirst(Task.class));
	}

	@Override
	public List<Task> getTasks(List<String> taskIds) {
		if (taskIds.isEmpty()) {
			return Lists.newArrayList();
		}
		return getWithTransaction(c -> getTasks(c, taskIds));
	}

	@Override
	public List<Task> getTasksForWorkflow(String workflowId) {
		String SQL = "SELECT task_id FROM task WHERE workflow_id = ?";
		return getWithTransaction(tx -> query(tx, SQL, q -> {
			List<String> taskIds = q.addParameter(workflowId).executeScalarList(String.class);
			return getTasks(tx, taskIds);
		}));
	}

	@Override
	public String createWorkflow(Workflow workflow) {
		return insertOrUpdateWorkflow(workflow, false);
	}

	@Override
	public String updateWorkflow(Workflow workflow) {
		return insertOrUpdateWorkflow(workflow, true);
	}

	@Override
	public void removeWorkflow(String workflowId) {
		Workflow workflow = getWorkflow(workflowId, true);
		if (workflow == null)
			return;

		withTransaction(connection -> {
			for (Task task : workflow.getTasks()) {
				removeTask(connection, task);
			}
			removeWorkflow(connection, workflowId);
		});
	}

	@Override
	@Deprecated
	public void removeFromPendingWorkflow(String workflowType, String workflowId) {
		// not in use any more. See references
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

		return null;
	}

	@Override
	public List<String> getRunningWorkflowIds(String workflowName) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		String SQL = "SELECT workflow_id FROM workflow WHERE workflow_type = ? AND workflow_status IN ('RUNNING','PAUSED')";

		return queryWithTransaction(SQL,
			q -> q.addParameter(workflowName).executeScalarList(String.class));
	}

	@Override
	public List<Workflow> getPendingWorkflowsByType(String workflowName) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		return getRunningWorkflowIds(workflowName).stream().map(this::getWorkflow).collect(Collectors.toList());
	}

	@Override
	public long getPendingWorkflowCount(String workflowName) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		String SQL = "SELECT COUNT(*) FROM workflow WHERE workflow_type = ? AND workflow_status IN ('RUNNING','PAUSED')";

		return queryWithTransaction(SQL, q -> q.addParameter(workflowName).executeCount());
	}

	@Override
	public long getInProgressTaskCount(String taskDefName) {
		String SQL = "SELECT COUNT(*) FROM task_in_progress WHERE task_def_name = ? AND in_progress = true";

		return queryWithTransaction(SQL, q -> q.addParameter(taskDefName).executeCount());
	}

	@Override
	public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
		Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
		Preconditions.checkNotNull(startTime, "startTime cannot be null");
		Preconditions.checkNotNull(endTime, "endTime cannot be null");

		List<Workflow> workflows = new LinkedList<>();

		withTransaction(tx -> {
			String SQL = "SELECT workflow_id FROM workflow WHERE workflow_type = ? AND date_str BETWEEN ? AND ?";

			List<String> workflowIds = query(tx, SQL, q -> q.addParameter(workflowName)
				.addParameter(dateStr(startTime)).addParameter(dateStr(endTime)).executeScalarList(String.class));
			workflowIds.forEach(workflowId -> {
				try {
					Workflow wf = getWorkflow(workflowId);
					if (wf.getCreateTime() >= startTime && wf.getCreateTime() <= endTime) {
						workflows.add(wf);
					}
				} catch (Exception e) {
					logger.error("Unable to load workflow id {} with name {}", workflowId, workflowName, e);
				}
			});
		});

		return workflows;
	}

	@Override
	public List<Workflow> getWorkflowsByCorrelationId(String correlationId) {
		Preconditions.checkNotNull(correlationId, "correlationId cannot be null");
		String SQL = "SELECT workflow_id FROM workflow WHERE correlation_id = ?";

		return queryWithTransaction(SQL,
			q -> q.addParameter(correlationId).executeScalarList(String.class).stream()
				.map(this::getWorkflow).collect(Collectors.toList()));
	}

	@Override
	public boolean addEventExecution(EventExecution ee) {
		return getWithTransaction(tx -> insertEventExecution(tx, ee));
	}

	@Override
	public void updateEventExecution(EventExecution ee) {
		withTransaction(tx -> updateEventExecution(tx, ee));
	}

	@Override
	@Deprecated
	public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max) {
		// not in use any more. See references
		return Collections.emptyList();
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
		withTransaction(tx -> insertOrUpdatePollData(tx, pollData, effectiveDomain));
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

	@Override
	public void addEventPublished(EventPublished ep) {
		getWithTransaction(tx -> insertEventPublished(tx, ep));
	}


	/**
	 * Function to find tasks in the workflows which associated with given tags
	 *
	 * Includes task into result if:
	 * workflow.tags contains ALL values from the tags parameter
	 * and task type matches the given task type
	 * and the task status is IN_PROGRESS
	 *
	 * @param tags A set of tags
	 * @return List of tasks
	 */
	@Override
	public List<Task> getPendingTasksByTags(String taskType, Set<String> tags) {
		String SQL = "SELECT t.json_data FROM task_in_progress tip " +
			"INNER JOIN task t ON t.task_id = tip.task_id " +
			"INNER JOIN workflow w ON w.workflow_id = tip.workflow_id " +
			"WHERE t.task_status = 'IN_PROGRESS' AND t.task_type = ? AND w.tags @> ?";

		return queryWithTransaction(SQL, q -> q.addParameter(taskType)
			.addParameter(tags)
			.executeAndFetch(Task.class));
	}

	/**
	 * Function to check is there any workflows associated with given tags
	 * Returns true if workflow.tags contains ALL values from the tags parameter
	 * Otherwise returns false
	 *
	 * @param tags A set of tags
	 * @return Either true or false
	 */
	@Override
	public boolean anyRunningWorkflowsByTags(Set<String> tags) {
		String SQL = "select count(*) from workflow where tags @> ?";
		return queryWithTransaction(SQL, q -> q.addParameter(tags).executeScalar(Long.class) > 0);
	}

	private static int dateStr(Long timeInMs) {
		Date date = new Date(timeInMs);

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		return Integer.parseInt(format.format(date));
	}

	private boolean addScheduledTask(Connection connection, Task task) {
		String taskKey = task.getReferenceTaskName() + task.getRetryCount();

		// Warning! Constraint name is also unique index name
		final String SQL = "INSERT INTO task_scheduled (workflow_id, task_key, task_id) " +
			"VALUES (?, ?, ?) ON CONFLICT ON CONSTRAINT task_scheduled_wf_task DO NOTHING";

		int count = query(connection, SQL, q -> q.addParameter(task.getWorkflowInstanceId())
			.addParameter(taskKey)
			.addParameter(task.getTaskId())
			.executeUpdate());

		return count > 0;
	}

	private void removeTask(Connection connection, Task task) {
		final String taskKey = task.getReferenceTaskName() + task.getRetryCount();

		removeScheduledTask(connection, task, taskKey);
		removeTaskInProgress(connection, task);
		removeTaskData(connection, task);
	}

	private void insertOrUpdateTask(Connection connection, Task task) {
		// Warning! Constraint name is also unique index name
		String SQL = "INSERT INTO task (task_id, task_type, task_status, json_data, workflow_id) VALUES (?, ?, ?, ?, ?) " +
			" ON CONFLICT ON CONSTRAINT task_task_id DO UPDATE SET modified_on=now(), task_status=?, json_data=?";
		execute(connection, SQL, q -> q.addParameter(task.getTaskId())
			.addParameter(task.getTaskType())
			.addParameter(task.getStatus().name())
			.addJsonParameter(task)
			.addParameter(task.getWorkflowInstanceId())
			.addParameter(task.getStatus().name())
			.addJsonParameter(task)
			.executeUpdate());
	}

	private void updateTask(Connection connection, Task task) {
		task.setUpdateTime(System.currentTimeMillis());
		if (task.getStatus() != null && task.getStatus().isTerminal()) {
			task.setEndTime(System.currentTimeMillis());
		}

		TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());
		if (taskDef != null && taskDef.concurrencyLimit() > 0) {
			updateInProgressStatus(connection, task);
		}

		insertOrUpdateTask(connection, task);

		if (task.getStatus() != null && task.getStatus().isTerminal()) {
			removeTaskInProgress(connection, task);
		}
	}

	private List<Task> getTasks(Connection connection, List<String> taskIds) {
		if (taskIds.isEmpty()) {
			return Lists.newArrayList();
		}

		// Generate a formatted query string with a variable number of bind params based
		// on taskIds.size()
		final String SQL = String.format("SELECT json_data FROM task WHERE task_id IN (%s) AND json_data IS NOT NULL",
			Query.generateInBindings(taskIds.size()));

		return query(connection, SQL, q -> q.addParameters(taskIds).executeAndFetch(Task.class));
	}

	private String insertOrUpdateWorkflow(Workflow workflow, boolean update) {
		Preconditions.checkNotNull(workflow, "workflow object cannot be null");

		if (workflow.getStatus().isTerminal()) {
			workflow.setEndTime(System.currentTimeMillis());
		}
		List<Task> tasks = workflow.getTasks();
		workflow.setTasks(Lists.newLinkedList());

		withTransaction(tx -> {
			if (update) {
				updateWorkflow(tx, workflow);
			} else {
				addWorkflow(tx, workflow);
			}
		});

		workflow.setTasks(tasks);

		return workflow.getWorkflowId();
	}

	private void addWorkflow(Connection connection, Workflow workflow) {
		String SQL = "INSERT INTO workflow (workflow_id, parent_workflow_id, workflow_type, workflow_status, " +
			"correlation_id, tags, input, json_data, date_str, start_time) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		execute(connection, SQL, q -> q.addParameter(workflow.getWorkflowId())
			.addParameter(workflow.getParentWorkflowId())
			.addParameter(workflow.getWorkflowType())
			.addParameter(workflow.getStatus().name())
			.addParameter(workflow.getCorrelationId())
			.addParameter(workflow.getTags())
			.addJsonParameter(workflow.getInput())
			.addJsonParameter(workflow)
			.addParameter(dateStr(workflow.getCreateTime()))
			.addTimestampParameter(workflow.getCreateTime())
			.executeUpdate());
	}

	private void updateWorkflow(Connection connection, Workflow workflow) {
		StringBuilder SQL = new StringBuilder();
		SQL.append("UPDATE workflow SET json_data = ?, workflow_status = ?, output = ?, end_time = ?");

		// We must not delete tags for RESET as it must be restarted right away
		if (workflow.getStatus().isTerminal() && workflow.getStatus() != Workflow.WorkflowStatus.RESET) {
			SQL.append(", tags = '{}'");
		}

		SQL.append(", modified_on = now() WHERE workflow_id = ?");

		execute(connection, SQL.toString(),
			q -> q.addJsonParameter(workflow)
				.addParameter(workflow.getStatus().name())
				.addJsonParameter(workflow.getOutput())
				.addTimestampParameter(workflow.getEndTime())
				.addParameter(workflow.getWorkflowId())
				.executeUpdate());
	}

	private Workflow readWorkflow(Connection connection, String workflowId) {
		String SQL = "SELECT json_data FROM workflow WHERE workflow_id = ?";

		return query(connection, SQL, q -> q.addParameter(workflowId).executeAndFetchFirst(Workflow.class));
	}

	private void removeWorkflow(Connection connection, String workflowId) {
		String SQL = "DELETE FROM workflow WHERE workflow_id = ?";

		execute(connection, SQL, q -> q.addParameter(workflowId).executeDelete());
	}

	private void addTaskInProgress(Connection connection, Task task) {
		String SQL = "SELECT true FROM task_in_progress WHERE task_def_name = ? AND task_id = ?";

		boolean exist = query(connection, SQL,
			q -> q.addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).exists());

		if (!exist) {
			SQL = "INSERT INTO task_in_progress (task_def_name, task_id, workflow_id) VALUES (?, ?, ?) " +
				"ON CONFLICT ON CONSTRAINT task_in_progress_fields DO NOTHING";

			execute(connection, SQL, q -> q.addParameter(task.getTaskDefName())
				.addParameter(task.getTaskId())
				.addParameter(task.getWorkflowInstanceId())
				.executeUpdate());
		}
	}

	private void removeTaskInProgress(Connection connection, Task task) {
		String SQL = "DELETE FROM task_in_progress WHERE task_def_name = ? AND task_id = ?";

		execute(connection, SQL,
			q -> q.addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).executeUpdate());
	}

	private void updateInProgressStatus(Connection connection, Task task) {
		boolean inProgress = Task.Status.IN_PROGRESS.equals(task.getStatus());

		String SQL = "UPDATE task_in_progress SET in_progress = ?, modified_on = now() "
			+ "WHERE task_def_name = ? AND task_id = ?";

		execute(connection, SQL, q -> q.addParameter(inProgress)
			.addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).executeUpdate());
	}

	private List<String> findAllTasksInProgressInOrderOfArrival(Task task, int limit) {
		String SQL = "SELECT task_id FROM task_in_progress WHERE task_def_name = ? ORDER BY id LIMIT ?";

		return queryWithTransaction(SQL,
			q -> q.addParameter(task.getTaskDefName()).addParameter(limit).executeScalarList(String.class));
	}

	private void removeScheduledTask(Connection connection, Task task, String taskKey) {
		String SQL = "DELETE FROM task_scheduled WHERE workflow_id = ? AND task_key = ?";

		execute(connection, SQL,
			q -> q.addParameter(task.getWorkflowInstanceId()).addParameter(taskKey).executeDelete());
	}

	private void removeTaskData(Connection connection, Task task) {
		String SQL = "DELETE FROM task WHERE task_id = ?";

		execute(connection, SQL, q -> q.addParameter(task.getTaskId()).executeDelete());
	}

	private boolean insertEventExecution(Connection connection, EventExecution ee) {
		String SQL = "INSERT INTO event_execution" +
			"(handler_name, event_name, message_id, execution_id, status, subject, received_on, accepted_on) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
			"ON CONFLICT ON CONSTRAINT event_execution_fields DO NOTHING";
		int count = query(connection, SQL, q -> q.addParameter(ee.getName())
			.addParameter(ee.getEvent())
			.addParameter(ee.getMessageId())
			.addParameter(ee.getId())
			.addParameter(ee.getStatus().name())
			.addParameter(ee.getSubject())
			.addTimestampParameter(ee.getReceived())
			.addTimestampParameter(ee.getAccepted())
			.executeUpdate());
		return count > 0;
	}

	private void updateEventExecution(Connection connection, EventExecution ee) {
		String SQL = "UPDATE event_execution SET " +
			"modified_on = now(), status = ?, started_on = ?, processed_on = ?" +
			"WHERE handler_name = ? AND event_name = ? " +
			"AND message_id = ? AND execution_id = ?";

		execute(connection, SQL, q -> q.addParameter(ee.getStatus().name())
			.addTimestampParameter(ee.getStarted())
			.addTimestampParameter(ee.getProcessed())
			.addParameter(ee.getName())
			.addParameter(ee.getEvent())
			.addParameter(ee.getMessageId())
			.addParameter(ee.getId())
			.executeUpdate());
	}

	private boolean insertEventPublished(Connection connection, EventPublished ep) {
		String SQL = "INSERT INTO event_published" +
			"(json_data, message_id, subject, published_on) " +
			"VALUES (?, ?, ?, ?)";
		int count = query(connection, SQL, q -> q.addJsonParameter(ep)
			.addParameter(ep.getId())
			.addParameter(ep.getSubject())
			.addTimestampParameter(ep.getPublished())
			.executeUpdate());
		return count > 0;
	}

	private void insertOrUpdatePollData(Connection connection, PollData pollData, String domain) {
		// Warning! Constraint name is also unique index name
		String SQL = "INSERT INTO poll_data (queue_name, domain, json_data) VALUES (?, ?, ?) " +
			"ON CONFLICT ON CONSTRAINT poll_data_fields DO UPDATE SET json_data=?, modified_on=now()";
		execute(connection, SQL, q -> q.addParameter(pollData.getQueueName())
			.addParameter(domain)
			.addJsonParameter(pollData)
			.addJsonParameter(pollData)
			.executeUpdate());
	}

	private PollData readPollData(Connection connection, String queueName, String domain) {
		String SQL = "SELECT json_data FROM poll_data WHERE queue_name = ? AND domain = ?";
		return query(connection, SQL,
			q -> q.addParameter(queueName).addParameter(domain).executeAndFetchFirst(PollData.class));
	}

	private List<PollData> readAllPollData(String queueName) {
		String SQL = "SELECT json_data FROM poll_data WHERE queue_name = ?";
		return queryWithTransaction(SQL, q -> q.addParameter(queueName).executeAndFetch(PollData.class));
	}
}

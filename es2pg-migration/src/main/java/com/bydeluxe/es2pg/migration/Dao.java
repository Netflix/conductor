package com.bydeluxe.es2pg.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.aurora.AuroraBaseDAO;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class Dao extends AuroraBaseDAO {
	private static final Set<String> queues = ConcurrentHashMap.newKeySet();

	Dao(DataSource dataSource, ObjectMapper mapper) {
		super(dataSource, mapper);
	}

	void upsertTask(Connection tx, Task task) {
		String SQL = "INSERT INTO task (created_on, modified_on, task_id, task_type, task_refname, task_status, " +
			"workflow_id, json_data, input, output, start_time, end_time) " +
			"VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT ON CONSTRAINT task_task_id DO " +
			"UPDATE SET created_on=?, modified_on=?, task_type=?, task_refname=?, task_status=?, " +
			"workflow_id=?, json_data=?, input=?, output=?, start_time=?, end_time=?";
		execute(tx, SQL, q -> q
			.addTimestampParameter(task.getStartTime(), System.currentTimeMillis())
			.addTimestampParameter(task.getUpdateTime(), System.currentTimeMillis())
			.addParameter(task.getTaskId())
			.addParameter(task.getTaskType())
			.addParameter(task.getReferenceTaskName())
			.addParameter(task.getStatus().name())
			.addParameter(task.getWorkflowInstanceId())
			.addJsonParameter(task)
			.addJsonParameter(task.getInputData())
			.addJsonParameter(task.getOutputData())
			.addTimestampParameter(task.getStartTime())
			.addTimestampParameter(task.getEndTime()) // end insert
			.addTimestampParameter(task.getStartTime(), System.currentTimeMillis())
			.addTimestampParameter(task.getUpdateTime(), System.currentTimeMillis())
			.addParameter(task.getTaskType())
			.addParameter(task.getReferenceTaskName())
			.addParameter(task.getStatus().name())
			.addParameter(task.getWorkflowInstanceId())
			.addJsonParameter(task)
			.addJsonParameter(task.getInputData())
			.addJsonParameter(task.getOutputData())
			.addTimestampParameter(task.getStartTime())
			.addTimestampParameter(task.getEndTime())
			.executeUpdate());

		addScheduledTask(tx, task);

		if (task.getStatus().isTerminal()) {
			removeTaskInProgress(tx, task);
		} else {
			addTaskInProgress(tx, task);
		}
	}

	void requeueSweep(Connection tx) {
		String SQL = "SELECT workflow_id FROM workflow WHERE workflow_status = 'RUNNING'";
		List<String> ids = query(tx, SQL, q -> q.executeAndFetch(String.class));
		ids.forEach(id -> pushMessage(tx, WorkflowExecutor.deciderQueue, id, null, 30));
	}

	private void createQueueIfNotExists(Connection tx, String queueName) {
		if (queues.contains(queueName)) {
			return;
		}
		final String SQL = "INSERT INTO queue (queue_name) VALUES (?) ON CONFLICT ON CONSTRAINT queue_name DO NOTHING";
		execute(tx, SQL, q -> q.addParameter(queueName.toLowerCase()).executeUpdate());
		queues.add(queueName);
	}

	void pushMessage(Connection tx, String queueName, String messageId, String payload, long offsetSeconds) {
		createQueueIfNotExists(tx, queueName);

		String SQL = "INSERT INTO queue_message (queue_name, message_id, popped, deliver_on, payload) " +
			"VALUES (?, ?, ?, ?, ?) ON CONFLICT ON CONSTRAINT queue_name_msg DO NOTHING";

		long deliverOn = System.currentTimeMillis() + (offsetSeconds * 1000);

		query(tx, SQL, q -> q.addParameter(queueName.toLowerCase())
			.addParameter(messageId)
			.addParameter(false)
			.addTimestampParameter(deliverOn)
			.addParameter(payload)
			.executeUpdate());
	}

	private void addTaskInProgress(Connection connection, Task task) {
		String SQL = "INSERT INTO task_in_progress (created_on, modified_on, task_def_name, task_id, workflow_id) " +
			" VALUES (?, ?, ?, ?, ?) ON CONFLICT ON CONSTRAINT task_in_progress_fields DO NOTHING";

		execute(connection, SQL, q -> q
			.addTimestampParameter(task.getStartTime(), System.currentTimeMillis())
			.addTimestampParameter(task.getUpdateTime(), System.currentTimeMillis())
			.addParameter(task.getTaskDefName())
			.addParameter(task.getTaskId())
			.addParameter(task.getWorkflowInstanceId())
			.executeUpdate());
	}

	private void removeTaskInProgress(Connection connection, Task task) {
		String SQL = "DELETE FROM task_in_progress WHERE task_def_name = ? AND task_id = ?";

		execute(connection, SQL,
			q -> q.addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).executeUpdate());
	}

	private void addScheduledTask(Connection connection, Task task) {
		String taskKey = task.getReferenceTaskName() + task.getRetryCount();

		final String SQL = "INSERT INTO task_scheduled (workflow_id, task_key, task_id) " +
			"VALUES (?, ?, ?) ON CONFLICT ON CONSTRAINT task_scheduled_wf_task DO NOTHING";

		query(connection, SQL, q -> q.addParameter(task.getWorkflowInstanceId())
			.addParameter(taskKey)
			.addParameter(task.getTaskId())
			.executeUpdate());
	}

	void upsertWorkflow(Connection tx, Workflow workflow) {
		String SQL = "INSERT INTO workflow (created_on, modified_on, start_time, end_time, parent_workflow_id, " +
			"workflow_id, workflow_type, workflow_status, date_str, json_data, input, output, correlation_id, tags) " +
			"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT ON CONSTRAINT workflow_workflow_id DO " +
			"UPDATE SET created_on=?, modified_on=?, start_time=?, end_time=?, parent_workflow_id=?, " +
			"workflow_type=?, workflow_status=?, date_str=?, json_data=?, input=?, output=?, correlation_id=?, tags=?";

		execute(tx, SQL, q -> q
			.addTimestampParameter(workflow.getCreateTime(), System.currentTimeMillis())
			.addTimestampParameter(workflow.getUpdateTime(), System.currentTimeMillis())
			.addTimestampParameter(workflow.getStartTime())
			.addTimestampParameter(workflow.getEndTime())
			.addParameter(workflow.getParentWorkflowId())
			.addParameter(workflow.getWorkflowId())
			.addParameter(workflow.getWorkflowType())
			.addParameter(workflow.getStatus().name())
			.addParameter(dateStr(workflow.getCreateTime()))
			.addJsonParameter(workflow)
			.addJsonParameter(workflow.getInput())
			.addJsonParameter(workflow.getOutput())
			.addParameter(workflow.getCorrelationId())
			.addParameter(workflow.getTags()) // end insert
			.addTimestampParameter(workflow.getCreateTime(), System.currentTimeMillis())
			.addTimestampParameter(workflow.getUpdateTime(), System.currentTimeMillis())
			.addTimestampParameter(workflow.getStartTime())
			.addTimestampParameter(workflow.getEndTime())
			.addParameter(workflow.getParentWorkflowId())
			.addParameter(workflow.getWorkflowType())
			.addParameter(workflow.getStatus().name())
			.addParameter(dateStr(workflow.getCreateTime()))
			.addJsonParameter(workflow)
			.addJsonParameter(workflow.getInput())
			.addJsonParameter(workflow.getOutput())
			.addParameter(workflow.getCorrelationId())
			.addParameter(workflow.getTags())
			.executeUpdate());
	}

	long workflowCount(Connection tx) {
		String SQL = "SELECT count(*) FROM workflow";
		return query(tx, SQL, q -> q.executeScalar(Long.class));
	}

	void upsertTaskDef(Connection tx, TaskDef def) {
		final String UPDATE_SQL = "UPDATE meta_task_def SET created_on = ?, modified_on = ?, json_data = ? WHERE name = ?";

		final String INSERT_SQL = "INSERT INTO meta_task_def (created_on, modified_on, name, json_data) VALUES (?, ?, ?, ?)";

		execute(tx, UPDATE_SQL, update -> {
			int result = update
				.addTimestampParameter(def.getCreateTime(), System.currentTimeMillis())
				.addTimestampParameter(def.getUpdateTime(), System.currentTimeMillis())
				.addJsonParameter(def)
				.addParameter(def.getName())
				.executeUpdate();

			if (result == 0) {
				execute(tx, INSERT_SQL,
					insert -> insert
						.addTimestampParameter(def.getCreateTime(), System.currentTimeMillis())
						.addTimestampParameter(def.getUpdateTime(), System.currentTimeMillis())
						.addParameter(def.getName())
						.addJsonParameter(def)
						.executeUpdate());
			}
		});
	}

	void upsertWorkflowDef(Connection tx, WorkflowDef def) {
		Optional<Integer> version = getLatestVersion(tx, def);
		if (!version.isPresent() || version.get() < def.getVersion()) {
			final String SQL = "INSERT INTO meta_workflow_def (created_on, modified_on, name, version, json_data) " +
				"VALUES (?, ?, ?, ?, ?)";

			execute(tx, SQL, q -> q
				.addTimestampParameter(def.getCreateTime(), System.currentTimeMillis())
				.addTimestampParameter(def.getUpdateTime(), System.currentTimeMillis())
				.addParameter(def.getName())
				.addParameter(def.getVersion())
				.addJsonParameter(def)
				.executeUpdate());
		} else {
			final String SQL = "UPDATE meta_workflow_def SET created_on = ?, modified_on = ?, json_data = ? " +
				"WHERE name = ? AND version = ?";

			execute(tx, SQL, q -> q
				.addTimestampParameter(def.getCreateTime(), System.currentTimeMillis())
				.addTimestampParameter(def.getUpdateTime(), System.currentTimeMillis())
				.addJsonParameter(def)
				.addParameter(def.getName())
				.addParameter(def.getVersion())
				.executeUpdate());
		}

		updateLatestVersion(tx, def);
	}

	private Optional<Integer> getLatestVersion(Connection tx, WorkflowDef def) {
		final String SQL = "SELECT max(version) AS version FROM meta_workflow_def WHERE name = ?";
		Integer val = query(tx, SQL, q -> q.addParameter(def.getName()).executeScalar(Integer.class));

		return Optional.ofNullable(val);
	}

	private void updateLatestVersion(Connection tx, WorkflowDef def) {
		final String SQL = "UPDATE meta_workflow_def SET latest_version = ? WHERE name = ?";

		execute(tx, SQL,
			q -> q.addParameter(def.getVersion()).addParameter(def.getName()).executeUpdate());
	}

	private static int dateStr(Long timeInMs) {
		Date date = new Date(timeInMs);

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		return Integer.parseInt(format.format(date));
	}
}

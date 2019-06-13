package com.bydeluxe.es2pg.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.aurora.AuroraBaseDAO;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;

import javax.sql.DataSource;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

class Dao extends AuroraBaseDAO {
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

	private static int dateStr(Long timeInMs) {
		Date date = new Date(timeInMs);

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		return Integer.parseInt(format.format(date));
	}
}

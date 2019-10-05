/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.dao.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.dao.sql.SQLExecutionDAO;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.sql.Connection;

@Singleton
public class MySQLExecutionDAO extends SQLExecutionDAO {

    @Inject
    public MySQLExecutionDAO(ObjectMapper objectMapper, DataSource dataSource) {
        super(objectMapper, dataSource);
    }

    protected void addPendingWorkflow(Connection connection, String workflowType, String workflowId) {

        String INSERT_PENDING_WORKFLOW = "INSERT IGNORE INTO workflow_pending (workflow_type, workflow_id) VALUES (?, ?)";

        execute(connection, INSERT_PENDING_WORKFLOW,
                q -> q.addParameter(workflowType).addParameter(workflowId).executeUpdate());

    }

    protected void insertOrUpdateTaskData(Connection connection, Task task) {

        String INSERT_TASK = "INSERT INTO task (task_id, json_data, modified_on) VALUES (?, ?, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE json_data=VALUES(json_data), modified_on=VALUES(modified_on)";
        execute(connection, INSERT_TASK, q -> q.addParameter(task.getTaskId()).addJsonParameter(task).executeUpdate());

    }

    protected void addWorkflowToTaskMapping(Connection connection, Task task) {

        String INSERT_WORKFLOW_TO_TASK = "INSERT IGNORE INTO workflow_to_task (workflow_id, task_id) VALUES (?, ?)";

        execute(connection, INSERT_WORKFLOW_TO_TASK,
                q -> q.addParameter(task.getWorkflowInstanceId()).addParameter(task.getTaskId()).executeUpdate());

    }

    @VisibleForTesting
    protected boolean addScheduledTask(Connection connection, Task task, String taskKey) {

        final String INSERT_IGNORE_SCHEDULED_TASK = "INSERT IGNORE INTO task_scheduled (workflow_id, task_key, task_id) VALUES (?, ?, ?)";

        int count = query(connection, INSERT_IGNORE_SCHEDULED_TASK, q -> q.addParameter(task.getWorkflowInstanceId())
                .addParameter(taskKey).addParameter(task.getTaskId()).executeUpdate());
        return count > 0;

    }

    protected void insertOrUpdatePollData(Connection connection, PollData pollData, String domain) {

        String INSERT_POLL_DATA = "INSERT INTO poll_data (queue_name, domain, json_data, modified_on) VALUES (?, ?, ?, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE json_data=VALUES(json_data), modified_on=VALUES(modified_on)";
        execute(connection, INSERT_POLL_DATA, q -> q.addParameter(pollData.getQueueName()).addParameter(domain)
                .addJsonParameter(pollData).executeUpdate());
    }
}

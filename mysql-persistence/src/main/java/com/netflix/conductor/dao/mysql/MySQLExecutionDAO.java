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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.metrics.Monitors;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class MySQLExecutionDAO extends MySQLBaseDAO implements ExecutionDAO {

    private static final String ARCHIVED_FIELD = "archived";
    private static final String RAW_JSON_FIELD = "rawJSON";

    @Inject
    public MySQLExecutionDAO(ObjectMapper objectMapper, DataSource dataSource) {
        super(objectMapper, dataSource);
    }

    private static String dateStr(Long timeInMs) {
        Date date = new Date(timeInMs);
        return dateStr(date);
    }

    private static String dateStr(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(date);
    }

    @Override
    public List<Task> getPendingTasksByWorkflow(String taskDefName, String workflowId) {
        // @formatter:off
        String GET_IN_PROGRESS_TASKS_FOR_WORKFLOW = "SELECT json_data FROM task_in_progress tip "
                + "INNER JOIN task t ON t.task_id = tip.task_id " + "WHERE task_def_name = ? AND workflow_id = ?";
        // @formatter:on

        return queryWithTransaction(GET_IN_PROGRESS_TASKS_FOR_WORKFLOW,
                q -> q.addParameter(taskDefName).addParameter(workflowId).executeAndFetch(Task.class));
    }

    @Override
    public List<Task> getTasks(String taskDefName, String startKey, int count) {
        List<Task> tasks = new ArrayList<>(count);

        List<Task> pendingTasks = getPendingTasksForTaskType(taskDefName);
        boolean startKeyFound = startKey == null;
        int found = 0;
        for (Task pendingTask : pendingTasks) {
            if (!startKeyFound) {
                if (pendingTask.getTaskId().equals(startKey)) {
                    startKeyFound = true;
                    // noinspection ConstantConditions
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

    private static String taskKey(Task task) {
        return task.getReferenceTaskName() + "_" + task.getRetryCount();
    }

    @Override
    public List<Task> createTasks(List<Task> tasks) {
        List<Task> created = Lists.newArrayListWithCapacity(tasks.size());

        withTransaction(connection -> {
            for (Task task : tasks) {
                validate(task);

                task.setScheduledTime(System.currentTimeMillis());

                final String taskKey = taskKey(task);

                boolean scheduledTaskAdded = addScheduledTask(connection, task, taskKey);

                if (!scheduledTaskAdded) {
                    logger.trace("Task already scheduled, skipping the run " + task.getTaskId() + ", ref="
                            + task.getReferenceTaskName() + ", key=" + taskKey);
                    continue;
                }

                insertOrUpdateTaskData(connection, task);
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

    /**
     * This is a dummy implementation and this feature is not for Mysql backed
     * Conductor
     *
     * @param task: which needs to be evaluated whether it is rateLimited or not
     * @return
     */
    @Override
    public boolean exceedsRateLimitPerFrequency(Task task) {
        return false;
    }

    @Override
    public boolean exceedsInProgressLimit(Task task) {

        Optional<TaskDef> taskDefinition = task.getTaskDefinition();
        if (!taskDefinition.isPresent()) {
            return false;
        }

        TaskDef taskDef = taskDefinition.get();

        int limit = taskDef.concurrencyLimit();
        if (limit <= 0) {
            return false;
        }

        long current = getInProgressTaskCount(task.getTaskDefName());

        if (current >= limit) {
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
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
            Monitors.recordTaskConcurrentExecutionLimited(task.getTaskDefName(), limit);
        }

        return rateLimited;
    }

    @Override
    public boolean removeTask(String taskId) {
        Task task = getTask(taskId);

        if (task == null) {
            logger.warn("No such task found by id {}", taskId);
            return false;
        }

        final String taskKey = taskKey(task);

        withTransaction(connection -> {
            removeScheduledTask(connection, task, taskKey);
            removeWorkflowToTaskMapping(connection, task);
            removeTaskInProgress(connection, task);
            removeTaskData(connection, task);
        });
        return true;
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
    public List<Task> getPendingTasksForTaskType(String taskName) {
        Preconditions.checkNotNull(taskName, "task name cannot be null");
        // @formatter:off
        String GET_IN_PROGRESS_TASKS_FOR_TYPE = "SELECT json_data FROM task_in_progress tip "
                + "INNER JOIN task t ON t.task_id = tip.task_id " + "WHERE task_def_name = ?";
        // @formatter:on

        return queryWithTransaction(GET_IN_PROGRESS_TASKS_FOR_TYPE,
                q -> q.addParameter(taskName).executeAndFetch(Task.class));
    }

    @Override
    public List<Task> getTasksForWorkflow(String workflowId) {
        String GET_TASKS_FOR_WORKFLOW = "SELECT task_id FROM workflow_to_task WHERE workflow_id = ?";
        return getWithTransaction(tx -> query(tx, GET_TASKS_FOR_WORKFLOW, q -> {
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
    public boolean removeWorkflow(String workflowId) {
        boolean removed = false;
        Workflow workflow = getWorkflow(workflowId, true);
        if (workflow != null) {
            withTransaction(connection -> {
                removeWorkflowDefToWorkflowMapping(connection, workflow);
                removeWorkflow(connection, workflowId);
                removePendingWorkflow(connection, workflow.getWorkflowName(), workflowId);
            });
            removed = true;

            for (Task task : workflow.getTasks()) {
                if (!removeTask(task.getTaskId())) {
                    removed = false;
                }
            }
        }
        return removed;
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
        }
        return workflow;
    }

    /**
     * @param workflowName name of the workflow
     * @param version the workflow version
     * @return list of workflow ids that are in RUNNING state
     * <em>returns workflows of all versions for the given workflow name</em>
     */
    @Override
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        String GET_PENDING_WORKFLOW_IDS = "SELECT workflow_id FROM workflow_pending WHERE workflow_type = ?";

        return queryWithTransaction(GET_PENDING_WORKFLOW_IDS,
                q -> q.addParameter(workflowName).executeScalarList(String.class));
    }

    /**
     * @param workflowName Name of the workflow
     * @param version the workflow version
     * @return list of workflows that are in RUNNING state
     */
    @Override
    public List<Workflow> getPendingWorkflowsByType(String workflowName, int version) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        return getRunningWorkflowIds(workflowName, version).stream()
                .map(this::getWorkflow)
                .filter(workflow -> workflow.getWorkflowVersion() == version)
                .collect(Collectors.toList());
    }

    @Override
    public long getPendingWorkflowCount(String workflowName) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        String GET_PENDING_WORKFLOW_COUNT = "SELECT COUNT(*) FROM workflow_pending WHERE workflow_type = ?";

        return queryWithTransaction(GET_PENDING_WORKFLOW_COUNT, q -> q.addParameter(workflowName).executeCount());
    }

    @Override
    public long getInProgressTaskCount(String taskDefName) {
        String GET_IN_PROGRESS_TASK_COUNT = "SELECT COUNT(*) FROM task_in_progress WHERE task_def_name = ? AND in_progress_status = true";

        return queryWithTransaction(GET_IN_PROGRESS_TASK_COUNT, q -> q.addParameter(taskDefName).executeCount());
    }

    @Override
    public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        Preconditions.checkNotNull(startTime, "startTime cannot be null");
        Preconditions.checkNotNull(endTime, "endTime cannot be null");

        List<Workflow> workflows = new LinkedList<>();

        withTransaction(tx -> {
            // @formatter:off
            String GET_ALL_WORKFLOWS_FOR_WORKFLOW_DEF = "SELECT workflow_id FROM workflow_def_to_workflow "
                    + "WHERE workflow_def = ? AND date_str BETWEEN ? AND ?";
            // @formatter:on

            List<String> workflowIds = query(tx, GET_ALL_WORKFLOWS_FOR_WORKFLOW_DEF, q -> q.addParameter(workflowName)
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
    public List<Workflow> getWorkflowsByCorrelationId(String correlationId, boolean includeTasks) {
        Preconditions.checkNotNull(correlationId, "correlationId cannot be null");
        String GET_WORKFLOWS_BY_CORRELATION_ID = "SELECT workflow_id FROM workflow WHERE correlation_id = ?";

        return queryWithTransaction(GET_WORKFLOWS_BY_CORRELATION_ID,
                q -> q.addParameter(correlationId).executeScalarList(String.class).stream()
                        .map(workflowId -> getWorkflow(workflowId, includeTasks)).collect(Collectors.toList()));
    }

    @Override
    public boolean canSearchAcrossWorkflows() {
        return true;
    }

    @Override
    public boolean addEventExecution(EventExecution eventExecution) {
        try {
            return getWithTransaction(tx -> insertEventExecution(tx, eventExecution));
        } catch (Exception e) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR,
                    "Unable to add event execution " + eventExecution.getId(), e);
        }
    }

    @Override
    public void removeEventExecution(EventExecution eventExecution) {
        try {
            withTransaction(tx -> removeEventExecution(tx, eventExecution));
        } catch (Exception e) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR,
                    "Unable to remove event execution " + eventExecution.getId(), e);
        }
    }

    @Override
    public void updateEventExecution(EventExecution eventExecution) {
        try {
            withTransaction(tx -> updateEventExecution(tx, eventExecution));
        } catch (Exception e) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR,
                    "Unable to update event execution " + eventExecution.getId(), e);
        }
    }

    @Override
    public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId,
                                                   int max) {
        try {
            List<EventExecution> executions = Lists.newLinkedList();
            withTransaction(tx -> {
                for (int i = 0; i < max; i++) {
                    String executionId = messageId + "_" + i; // see SimpleEventProcessor.handle to understand how the
                    // execution id is set
                    EventExecution ee = readEventExecution(tx, eventHandlerName, eventName, messageId, executionId);
                    if (ee == null) {
                        break;
                    }
                    executions.add(ee);
                }
            });
            return executions;
        } catch (Exception e) {
            String message = String.format(
                    "Unable to get event executions for eventHandlerName=%s, eventName=%s, messageId=%s",
                    eventHandlerName, eventName, messageId);
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, message, e);
        }
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

    private List<Task> getTasks(Connection connection, List<String> taskIds) {
        if (taskIds.isEmpty()) {
            return Lists.newArrayList();
        }

        // Generate a formatted query string with a variable number of bind params based
        // on taskIds.size()
        final String GET_TASKS_FOR_IDS = String.format(
                "SELECT json_data FROM task WHERE task_id IN (%s) AND json_data IS NOT NULL",
                Query.generateInBindings(taskIds.size()));

        return query(connection, GET_TASKS_FOR_IDS, q -> q.addParameters(taskIds).executeAndFetch(Task.class));
    }

    private String insertOrUpdateWorkflow(Workflow workflow, boolean update) {
        Preconditions.checkNotNull(workflow, "workflow object cannot be null");

        boolean terminal = workflow.getStatus().isTerminal();

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
                removePendingWorkflow(tx, workflow.getWorkflowName(), workflow.getWorkflowId());
            } else {
                addPendingWorkflow(tx, workflow.getWorkflowName(), workflow.getWorkflowId());
            }
        });

        workflow.setTasks(tasks);
        return workflow.getWorkflowId();
    }

    private void updateTask(Connection connection, Task task) {
        Optional<TaskDef> taskDefinition = task.getTaskDefinition();

        if (taskDefinition.isPresent() && taskDefinition.get().concurrencyLimit() > 0) {
            boolean inProgress = task.getStatus() != null && task.getStatus().equals(Task.Status.IN_PROGRESS);
            updateInProgressStatus(connection, task, inProgress);
        }

        insertOrUpdateTaskData(connection, task);

        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            removeTaskInProgress(connection, task);
        }

        addWorkflowToTaskMapping(connection, task);
    }

    private Workflow readWorkflow(Connection connection, String workflowId) {
        String GET_WORKFLOW = "SELECT json_data FROM workflow WHERE workflow_id = ?";

        return query(connection, GET_WORKFLOW, q -> q.addParameter(workflowId).executeAndFetchFirst(Workflow.class));
    }

    private void addWorkflow(Connection connection, Workflow workflow) {
        String INSERT_WORKFLOW = "INSERT INTO workflow (workflow_id, correlation_id, json_data) VALUES (?, ?, ?)";

        execute(connection, INSERT_WORKFLOW, q -> q.addParameter(workflow.getWorkflowId())
                .addParameter(workflow.getCorrelationId()).addJsonParameter(workflow).executeUpdate());
    }

    private void updateWorkflow(Connection connection, Workflow workflow) {
        String UPDATE_WORKFLOW = "UPDATE workflow SET json_data = ?, modified_on = CURRENT_TIMESTAMP WHERE workflow_id = ?";

        execute(connection, UPDATE_WORKFLOW,
                q -> q.addJsonParameter(workflow).addParameter(workflow.getWorkflowId()).executeUpdate());
    }

    private void removeWorkflow(Connection connection, String workflowId) {
        String REMOVE_WORKFLOW = "DELETE FROM workflow WHERE workflow_id = ?";
        execute(connection, REMOVE_WORKFLOW, q -> q.addParameter(workflowId).executeDelete());
    }

    private void addPendingWorkflow(Connection connection, String workflowType, String workflowId) {

        String INSERT_PENDING_WORKFLOW = "INSERT IGNORE INTO workflow_pending (workflow_type, workflow_id) VALUES (?, ?)";

        execute(connection, INSERT_PENDING_WORKFLOW,
                q -> q.addParameter(workflowType).addParameter(workflowId).executeUpdate());

    }

    private void removePendingWorkflow(Connection connection, String workflowType, String workflowId) {
        String REMOVE_PENDING_WORKFLOW = "DELETE FROM workflow_pending WHERE workflow_type = ? AND workflow_id = ?";

        execute(connection, REMOVE_PENDING_WORKFLOW,
                q -> q.addParameter(workflowType).addParameter(workflowId).executeDelete());
    }

    private void insertOrUpdateTaskData(Connection connection, Task task) {

        String INSERT_TASK = "INSERT INTO task (task_id, json_data, modified_on) VALUES (?, ?, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE json_data=VALUES(json_data), modified_on=VALUES(modified_on)";
        execute(connection, INSERT_TASK, q -> q.addParameter(task.getTaskId()).addJsonParameter(task).executeUpdate());

    }

    private void removeTaskData(Connection connection, Task task) {
        String REMOVE_TASK = "DELETE FROM task WHERE task_id = ?";
        execute(connection, REMOVE_TASK, q -> q.addParameter(task.getTaskId()).executeDelete());
    }

    private void addWorkflowToTaskMapping(Connection connection, Task task) {

        String INSERT_WORKFLOW_TO_TASK = "INSERT IGNORE INTO workflow_to_task (workflow_id, task_id) VALUES (?, ?)";

        execute(connection, INSERT_WORKFLOW_TO_TASK,
                q -> q.addParameter(task.getWorkflowInstanceId()).addParameter(task.getTaskId()).executeUpdate());

    }

    private void removeWorkflowToTaskMapping(Connection connection, Task task) {
        String REMOVE_WORKFLOW_TO_TASK = "DELETE FROM workflow_to_task WHERE workflow_id = ? AND task_id = ?";

        execute(connection, REMOVE_WORKFLOW_TO_TASK,
                q -> q.addParameter(task.getWorkflowInstanceId()).addParameter(task.getTaskId()).executeDelete());
    }

    private void addWorkflowDefToWorkflowMapping(Connection connection, Workflow workflow) {
        String INSERT_WORKFLOW_DEF_TO_WORKFLOW = "INSERT INTO workflow_def_to_workflow (workflow_def, date_str, workflow_id) VALUES (?, ?, ?)";

        execute(connection, INSERT_WORKFLOW_DEF_TO_WORKFLOW,
                q -> q.addParameter(workflow.getWorkflowName()).addParameter(dateStr(workflow.getCreateTime()))
                        .addParameter(workflow.getWorkflowId()).executeUpdate());
    }

    private void removeWorkflowDefToWorkflowMapping(Connection connection, Workflow workflow) {
        String REMOVE_WORKFLOW_DEF_TO_WORKFLOW = "DELETE FROM workflow_def_to_workflow WHERE workflow_def = ? AND date_str = ? AND workflow_id = ?";

        execute(connection, REMOVE_WORKFLOW_DEF_TO_WORKFLOW,
                q -> q.addParameter(workflow.getWorkflowName()).addParameter(dateStr(workflow.getCreateTime()))
                        .addParameter(workflow.getWorkflowId()).executeUpdate());
    }

    @VisibleForTesting
    boolean addScheduledTask(Connection connection, Task task, String taskKey) {

        final String INSERT_IGNORE_SCHEDULED_TASK = "INSERT IGNORE INTO task_scheduled (workflow_id, task_key, task_id) VALUES (?, ?, ?)";

        int count = query(connection, INSERT_IGNORE_SCHEDULED_TASK, q -> q.addParameter(task.getWorkflowInstanceId())
                .addParameter(taskKey).addParameter(task.getTaskId()).executeUpdate());
        return count > 0;

    }

    private void removeScheduledTask(Connection connection, Task task, String taskKey) {
        String REMOVE_SCHEDULED_TASK = "DELETE FROM task_scheduled WHERE workflow_id = ? AND task_key = ?";
        execute(connection, REMOVE_SCHEDULED_TASK,
                q -> q.addParameter(task.getWorkflowInstanceId()).addParameter(taskKey).executeDelete());
    }

    private void addTaskInProgress(Connection connection, Task task) {
        String EXISTS_IN_PROGRESS_TASK = "SELECT EXISTS(SELECT 1 FROM task_in_progress WHERE task_def_name = ? AND task_id = ?)";

        boolean exist = query(connection, EXISTS_IN_PROGRESS_TASK,
                q -> q.addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).exists());

        if (!exist) {
            String INSERT_IN_PROGRESS_TASK = "INSERT INTO task_in_progress (task_def_name, task_id, workflow_id) VALUES (?, ?, ?)";

            execute(connection, INSERT_IN_PROGRESS_TASK, q -> q.addParameter(task.getTaskDefName())
                    .addParameter(task.getTaskId()).addParameter(task.getWorkflowInstanceId()).executeUpdate());
        }
    }

    private void removeTaskInProgress(Connection connection, Task task) {
        String REMOVE_IN_PROGRESS_TASK = "DELETE FROM task_in_progress WHERE task_def_name = ? AND task_id = ?";

        execute(connection, REMOVE_IN_PROGRESS_TASK,
                q -> q.addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).executeUpdate());
    }

    private void updateInProgressStatus(Connection connection, Task task, boolean inProgress) {
        String UPDATE_IN_PROGRESS_TASK_STATUS = "UPDATE task_in_progress SET in_progress_status = ?, modified_on = CURRENT_TIMESTAMP "
                + "WHERE task_def_name = ? AND task_id = ?";

        execute(connection, UPDATE_IN_PROGRESS_TASK_STATUS, q -> q.addParameter(inProgress)
                .addParameter(task.getTaskDefName()).addParameter(task.getTaskId()).executeUpdate());
    }

    private boolean insertEventExecution(Connection connection, EventExecution eventExecution) {

        String INSERT_EVENT_EXECUTION = "INSERT INTO event_execution (event_handler_name, event_name, message_id, execution_id, json_data) "
                + "VALUES (?, ?, ?, ?, ?)";
        int count = query(connection, INSERT_EVENT_EXECUTION,
                q -> q.addParameter(eventExecution.getName()).addParameter(eventExecution.getEvent())
                        .addParameter(eventExecution.getMessageId()).addParameter(eventExecution.getId())
                        .addJsonParameter(eventExecution).executeUpdate());
        return count > 0;
    }

    private void updateEventExecution(Connection connection, EventExecution eventExecution) {
        // @formatter:off
        String UPDATE_EVENT_EXECUTION = "UPDATE event_execution SET " + "json_data = ?, "
                + "modified_on = CURRENT_TIMESTAMP " + "WHERE event_handler_name = ? " + "AND event_name = ? "
                + "AND message_id = ? " + "AND execution_id = ?";
        // @formatter:on

        execute(connection, UPDATE_EVENT_EXECUTION,
                q -> q.addJsonParameter(eventExecution).addParameter(eventExecution.getName())
                        .addParameter(eventExecution.getEvent()).addParameter(eventExecution.getMessageId())
                        .addParameter(eventExecution.getId()).executeUpdate());
    }

    private void removeEventExecution(Connection connection, EventExecution eventExecution) {
        String REMOVE_EVENT_EXECUTION = "DELETE FROM event_execution " + "WHERE event_handler_name = ? "
                + "AND event_name = ? " + "AND message_id = ? " + "AND execution_id = ?";

        execute(connection, REMOVE_EVENT_EXECUTION,
                q -> q.addParameter(eventExecution.getName()).addParameter(eventExecution.getEvent())
                        .addParameter(eventExecution.getMessageId()).addParameter(eventExecution.getId())
                        .executeUpdate());
    }

    private EventExecution readEventExecution(Connection connection, String eventHandlerName, String eventName,
                                              String messageId, String executionId) {
        // @formatter:off
        String GET_EVENT_EXECUTION = "SELECT json_data FROM event_execution " + "WHERE event_handler_name = ? "
                + "AND event_name = ? " + "AND message_id = ? " + "AND execution_id = ?";
        // @formatter:on
        return query(connection, GET_EVENT_EXECUTION, q -> q.addParameter(eventHandlerName).addParameter(eventName)
                .addParameter(messageId).addParameter(executionId).executeAndFetchFirst(EventExecution.class));
    }

    private void insertOrUpdatePollData(Connection connection, PollData pollData, String domain) {

        String INSERT_POLL_DATA = "INSERT INTO poll_data (queue_name, domain, json_data, modified_on) VALUES (?, ?, ?, CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE json_data=VALUES(json_data), modified_on=VALUES(modified_on)";
        execute(connection, INSERT_POLL_DATA, q -> q.addParameter(pollData.getQueueName()).addParameter(domain)
                .addJsonParameter(pollData).executeUpdate());
    }

    private PollData readPollData(Connection connection, String queueName, String domain) {
        String GET_POLL_DATA = "SELECT json_data FROM poll_data WHERE queue_name = ? AND domain = ?";
        return query(connection, GET_POLL_DATA,
                q -> q.addParameter(queueName).addParameter(domain).executeAndFetchFirst(PollData.class));
    }

    private List<PollData> readAllPollData(String queueName) {
        String GET_ALL_POLL_DATA = "SELECT json_data FROM poll_data WHERE queue_name = ?";
        return queryWithTransaction(GET_ALL_POLL_DATA, q -> q.addParameter(queueName).executeAndFetch(PollData.class));
    }

    private List<String> findAllTasksInProgressInOrderOfArrival(Task task, int limit) {
        String GET_IN_PROGRESS_TASKS_WITH_LIMIT = "SELECT task_id FROM task_in_progress WHERE task_def_name = ? ORDER BY id LIMIT ?";

        return queryWithTransaction(GET_IN_PROGRESS_TASKS_WITH_LIMIT,
                q -> q.addParameter(task.getTaskDefName()).addParameter(limit).executeScalarList(String.class));
    }

    private void validate(Task task) {
        Preconditions.checkNotNull(task, "task object cannot be null");
        Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
        Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
        Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");
    }
}

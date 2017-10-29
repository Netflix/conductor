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
package com.netflix.conductor.dao.es5;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5ExecutionDAO extends ElasticSearch5BaseDAO implements ExecutionDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5ExecutionDAO.class);
    private static final String ARCHIVED_FIELD = "archived";
    private static final String RAW_JSON_FIELD = "rawJSON";
    // Keys Families
    private static final String TASK_LIMIT_BUCKET = "TASK_LIMIT_BUCKET";
    private final static String IN_PROGRESS_TASKS = "IN_PROGRESS_TASKS";
    private final static String TASKS_IN_PROGRESS_STATUS = "TASKS_IN_PROGRESS_STATUS";
    private final static String WORKFLOW_TO_TASKS = "WORKFLOW_TO_TASKS";
    private final static String SCHEDULED_TASKS = "SCHEDULED_TASKS";
    private final static String TASK = "TASK";
    private final static String WORKFLOW = "WORKFLOW";
    private final static String PENDING_WORKFLOWS = "PENDING_WORKFLOWS";
    private final static String WORKFLOW_DEF_TO_WORKFLOWS = "DEF_TO_WORKFLOW";
    private final static String CORR_ID_TO_WORKFLOWS = "CORRID_TO_WORKFLOW";
    private final static String POLL_DATA = "POLL_DATA";
    private final static String EVENT_EXECUTION = "EVENT_EXECUTION";

    private MetadataDAO metadata;
    private IndexDAO indexer;

    @Inject
    public ElasticSearch5ExecutionDAO(Client client, Configuration config, ObjectMapper mapper, IndexDAO indexer, MetadataDAO metadata) {
        super(client, config, mapper, "runtime");
        this.indexer = indexer;
        this.metadata = metadata;
    }

    @Override
    public List<Task> getPendingTasksByWorkflow(String taskName, String workflowId) {
        logger.debug("getPendingTasksByWorkflow: taskName={}, workflowId={}", taskName, workflowId);
        List<Task> tasks = new LinkedList<>();

        List<Task> pendingTasks = getPendingTasksForTaskType(taskName);
        pendingTasks.forEach(pendingTask -> {
            if (pendingTask.getWorkflowInstanceId().equals(workflowId)) {
                tasks.add(pendingTask);
            }
        });

        return tasks;
    }

    @Override // TODO Review the code below!
    public List<Task> getTasks(String taskDefName, String startKey, int count) {
        logger.debug("getTasks: taskDefName={}, startKey={}, count={}", taskDefName, startKey, count);
        List<Task> tasks = new LinkedList<>();

        List<Task> pendingTasks = getPendingTasksForTaskType(taskDefName);
        boolean startKeyFound = (startKey == null) ? true : false;
        int foundcount = 0;
        for (int i = 0; i < pendingTasks.size(); i++) {
            if (!startKeyFound) {
                if (pendingTasks.get(i).getTaskId().equals(startKey)) {
                    startKeyFound = true;
                    if (startKey != null) {
                        continue;
                    }
                }
            }
            if (startKeyFound && foundcount < count) {
                tasks.add(pendingTasks.get(i));
                foundcount++;
            }
        }
        return tasks;
    }

    @Override
    public List<Task> createTasks(List<Task> tasks) {
        logger.debug("createTasks: tasks={}", toJson(tasks));
        List<Task> created = new LinkedList<>();

        for (Task task : tasks) {

            Preconditions.checkNotNull(task, "task object cannot be null");
            Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
            Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
            Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");

            task.setScheduledTime(System.currentTimeMillis());

            String indexName = toIndexName(SCHEDULED_TASKS);
            String typeName = toTypeName(task.getTaskDefName());
            String taskKey = task.getReferenceTaskName() + "" + task.getRetryCount();
            String id = toId(task.getWorkflowInstanceId(), taskKey);

            if (exists(indexName, typeName, id)) {
                logger.debug("Task already scheduled, skipping the run " + task.getTaskId() + ", ref=" + task.getReferenceTaskName() + ", key=" + taskKey);
                continue;
            }
            // SCHEDULED_TASKS
            Map payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(),
                    "taskRefName", task.getReferenceTaskName(),
                    "taskId", task.getTaskId());
            insert(indexName, typeName, id, payload);

            // WORKFLOW_TO_TASKS
            indexName = toIndexName(WORKFLOW_TO_TASKS);
            typeName = toTypeName(task.getTaskDefName());
            id = toId(task.getWorkflowInstanceId(), task.getTaskId());
            payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(), "taskId", task.getTaskId());
            insert(indexName, typeName, id, payload);

            // IN_PROGRESS_TASKS
            indexName = toIndexName(IN_PROGRESS_TASKS);
            typeName = toTypeName(task.getTaskDefName());
            id = toId(task.getTaskDefName(), task.getTaskId());
            payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(),
                    "taskDefName", task.getTaskDefName(),
                    "taskId", task.getTaskId());
            insert(indexName, typeName, id, payload);

            updateTask(task);
            created.add(task);
        }

        return created;
    }

    @Override
    public void updateTask(Task task) {
        logger.debug("updateTask: task={}", toJson(task));
        task.setUpdateTime(System.currentTimeMillis());
        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            task.setEndTime(System.currentTimeMillis());
        }

        TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());

        if (taskDef != null && taskDef.concurrencyLimit() > 0) {
            String indexName = toIndexName(TASKS_IN_PROGRESS_STATUS);
            String typeName = toTypeName(task.getTaskDefName());
            String id = toId(task.getTaskDefName(), task.getTaskId());

            if (task.getStatus() != null && task.getStatus().equals(Task.Status.IN_PROGRESS)) {
                Map<String, Object> payload = ImmutableMap.of("workflowId", task.getWorkflowInstanceId(),
                        "taskDefName", task.getTaskDefName(),
                        "taskId", task.getTaskId());
                insert(indexName, typeName, id, payload);
            } else {
                delete(indexName, typeName, id);
                delete(toIndexName(TASK_LIMIT_BUCKET), toTypeName(task.getTaskDefName()), id);
            }
        }

        String indexName = toIndexName(TASK);
        String typeName = toTypeName(task.getTaskDefName());
        String id = toId(task.getTaskId());
        insert(indexName, typeName, id, toMap(task));
        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            indexName = toIndexName(IN_PROGRESS_TASKS);
            typeName = toTypeName(task.getTaskDefName());
            id = toId(task.getTaskDefName(), task.getTaskId());
            delete(indexName, typeName, id);
        }

        indexer.index(task);
    }

    @Override // TODO Review and complete
    public boolean exceedsInProgressLimit(Task task) {
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
            Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
            return true;
        }

        String indexName = toIndexName(TASK_LIMIT_BUCKET);
        String typeName = toTypeName(task.getTaskDefName());
        String id = toId(task.getTaskDefName(), task.getTaskId());

        double score = System.currentTimeMillis();

        Map<String, Object> payload = ImmutableMap.of("score", score, "taskId", task.getTaskId(),
                "taskDefName", task.getTaskDefName());

        // Adds all the specified members with the specified scores to the sorted set stored at key.
        //String rateLimitKey = nsKey(TASK_LIMIT_BUCKET, task.getTaskDefName());
        //dynoClient.zaddnx(rateLimitKey, score, taskId); //NX - Don't update already existing elements. Always add new elements.
        insert(indexName, typeName, id, payload);

        QueryBuilder nameQuery = QueryBuilders.matchQuery("taskDefName", task.getTaskDefName());
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery("score").gte(0).lte(score + 1);
        QueryBuilder finalQuery = QueryBuilders.boolQuery().must(nameQuery).must(rangeQuery);

        // Search ids where the score between 0 and score + 1 using limit
        //Set<String> ids = dynoClient.zrangeByScore(rateLimitKey, 0, score + 1, limit);
        List<String> ids = findIds(indexName, finalQuery, limit);

        // TODO Review this logic in the original DAO! Does it work at all ?
        boolean rateLimited = !ids.contains(task.getTaskId());
        if (rateLimited) {
            logger.info("Task execution count limited. {}, limit {}, current {}", task.getTaskDefName(), limit, getInProgressTaskCount(task.getTaskDefName()));
            //Cleanup any items that are still present in the rate limit bucket but not in progress anymore!
            //String inProgressKey = nsKey(TASKS_IN_PROGRESS_STATUS, task.getTaskDefName());
            //ids.stream().filter(id -> !dynoClient.sismember(inProgressKey, id)).forEach(id2 -> dynoClient.zrem(rateLimitKey, id2));

            Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
        }
        return rateLimited;
    }

    @Override
    public void addTaskExecLog(List<TaskExecLog> log) {
        logger.debug("addTaskExecLog: log={}", toJson(log));
        indexer.add(log);
    }

    @Override
    public void updateTasks(List<Task> tasks) {
        logger.debug("updateTasks: tasks={}", toJson(tasks));
        for (Task task : tasks) {
            updateTask(task);
        }
    }

    @Override
    public void removeTask(String taskId) {
        logger.debug("removeTask: taskId={}", taskId);
        Task task = getTask(taskId);
        if (task == null) {
            logger.warn("No such Task by id {}", taskId);
            return;
        }
        String taskKey = task.getReferenceTaskName() + String.valueOf(task.getRetryCount());

        delete(toIndexName(SCHEDULED_TASKS), toTypeName(task.getTaskDefName()), toId(task.getWorkflowInstanceId(), taskKey));
        delete(toIndexName(IN_PROGRESS_TASKS), toTypeName(task.getTaskDefName()), toId(task.getTaskDefName(), taskId));
        delete(toIndexName(WORKFLOW_TO_TASKS), toTypeName(task.getTaskDefName()), toId(task.getWorkflowInstanceId(), taskId));
        delete(toIndexName(TASKS_IN_PROGRESS_STATUS), toTypeName(task.getTaskDefName()), toId(task.getTaskDefName(), taskId));
        delete(toIndexName(TASK), toTypeName(task.getTaskDefName()), taskId);
        delete(toIndexName(TASK_LIMIT_BUCKET), toTypeName(task.getTaskDefName()), toId(task.getTaskDefName(), taskId));
        logger.debug("removeTask: done");
    }

    @Override
    public Task getTask(String taskId) {
        logger.debug("getTask: taskId={}", taskId);
        Preconditions.checkNotNull(taskId, "taskId name cannot be null");

        QueryBuilder idQuery = QueryBuilders.idsQuery().addIds(taskId);
        Task task = findOne(toIndexName(TASK), idQuery, Task.class);

        logger.debug("getTask: result={}", toJson(task));
        return task;
    }

    @Override
    public List<Task> getTasks(List<String> taskIds) {
        logger.debug("getTasks: taskIds={}", taskIds);

        IdsQueryBuilder idsQuery = QueryBuilders.idsQuery();
        idsQuery.ids().addAll(taskIds);

        List<Task> tasks = findAll(toIndexName(TASK), idsQuery, Task.class);

        logger.debug("getTasks: result={}", toJson(tasks));
        return tasks;
    }

    @Override
    public List<Task> getPendingTasksForTaskType(String taskDefName) {
        logger.debug("getPendingTasksForTaskType: taskDefName={}", taskDefName);
        Preconditions.checkNotNull(taskDefName, "task def name cannot be null");

        QueryBuilder query = QueryBuilders.matchQuery("taskDefName", taskDefName);
        List<Task> tasks = findAll(toIndexName(IN_PROGRESS_TASKS), toTypeName(taskDefName), query, Task.class);

        logger.debug("getPendingTasksForTaskType: result={}", toJson(tasks));
        return tasks;
    }

    @Override
    public List<Task> getTasksForWorkflow(String workflowId) {
        logger.debug("getTasksForWorkflow: workflowId={}", workflowId);
        Preconditions.checkNotNull(workflowId, "workflowId cannot be null");

        QueryBuilder query = QueryBuilders.matchQuery("workflowId", workflowId);
        List<HashMap> wraps = findAll(toIndexName(WORKFLOW_TO_TASKS), query, HashMap.class);
        Set<String> taskIds = wraps.stream().map(map -> (String) map.get("taskId")).collect(Collectors.toSet());
        List<Task> tasks = taskIds.stream().map(this::getTask).filter(Objects::nonNull).collect(Collectors.toList());
        logger.debug("getTasksForWorkflow: result={}", toJson(tasks));

        return tasks;
    }

    @Override
    public String createWorkflow(Workflow workflow) {
        logger.debug("createWorkflow: workflow={}", toJson(workflow));
        workflow.setCreateTime(System.currentTimeMillis());
        return insertOrUpdateWorkflow(workflow, false);
    }

    @Override
    public String updateWorkflow(Workflow workflow) {
        logger.debug("updateWorkflow: workflow={}", toJson(workflow));
        workflow.setUpdateTime(System.currentTimeMillis());
        return insertOrUpdateWorkflow(workflow, true);
    }

    @Override
    public void removeWorkflow(String workflowId) {
        logger.debug("removeWorkflow: workflowId={}", workflowId);
        try {

            Workflow wf = getWorkflow(workflowId, true);

            //Add to elasticsearch
            indexer.update(workflowId, new String[]{RAW_JSON_FIELD, ARCHIVED_FIELD}, new Object[]{toJson(wf), true});

            //String key = nsKey(WORKFLOW_DEF_TO_WORKFLOWS, wf.getWorkflowType(), dateStr(wf.getCreateTime()));
            String id = toId(wf.getWorkflowType(), dateStr(wf.getCreateTime()), wf.getWorkflowId());

            //dynoClient.srem(key, workflowId);
            delete(toIndexName(WORKFLOW_DEF_TO_WORKFLOWS), toTypeName(wf.getWorkflowType()), id);

            //dynoClient.srem(nsKey(CORR_ID_TO_WORKFLOWS, wf.getCorrelationId()), workflowId);
            delete(toIndexName(CORR_ID_TO_WORKFLOWS), toTypeName(wf.getWorkflowType()), toId(wf.getCorrelationId(), wf.getWorkflowId()));

            //dynoClient.srem(nsKey(PENDING_WORKFLOWS, wf.getWorkflowType()), workflowId);
            delete(toIndexName(PENDING_WORKFLOWS), toTypeName(wf.getWorkflowType()), toId(wf.getWorkflowType(), wf.getWorkflowId()));

            //dynoClient.del(nsKey(WORKFLOW, workflowId));
            delete(toIndexName(WORKFLOW), toTypeName(wf.getWorkflowType()), wf.getWorkflowId());
            for (Task task : wf.getTasks()) {
                removeTask(task.getTaskId());
            }
        } catch (Exception ex) {
            logger.debug("removeWorkflow: failed for {} with {}", workflowId, ex.getMessage(), ex);
            throw new ApplicationException(ex.getMessage(), ex);
        }
        logger.debug("removeWorkflow: done");
    }

    @Override
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {
        logger.debug("removeFromPendingWorkflow: workflowType={}, workflowId={}", workflowType, workflowId);
        delete(toIndexName(PENDING_WORKFLOWS), toTypeName(workflowType), toId(workflowType, workflowId));
        logger.debug("removeFromPendingWorkflow: done");
    }

    @Override
    public Workflow getWorkflow(String workflowId) {
        logger.debug("getWorkflow: workflowId={}", workflowId);
        return getWorkflow(workflowId, true);
    }

    @Override
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        logger.debug("getWorkflow: workflowId={}, includeTasks={}", workflowId, includeTasks);

        QueryBuilder query = QueryBuilders.idsQuery().addIds(workflowId);
        Workflow workflow = findOne(toIndexName(WORKFLOW), query, Workflow.class);
        if (workflow != null) {
            if (includeTasks) {
                List<Task> tasks = getTasksForWorkflow(workflowId);
                tasks.sort(Comparator.comparingLong(Task::getScheduledTime).thenComparingInt(Task::getSeq));
                workflow.setTasks(tasks);
            }

            logger.debug("getWorkflow: result(1)={}", toJson(workflow));
            return workflow;
        }

        // try from the archive
        String json = indexer.get(workflowId, RAW_JSON_FIELD);
        if (json == null) {
            throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "No such workflow found by id: " + workflowId);
        }
        workflow = convert(json, Workflow.class);
        if (!includeTasks) {
            workflow.getTasks().clear();
        }

        logger.debug("getWorkflow: result(2)={}", toJson(workflow));
        return workflow;
    }

    @Override
    public List<String> getRunningWorkflowIds(String workflowName) {
        logger.debug("getRunningWorkflowIds: workflowName={}", workflowName);
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");

        QueryBuilder query = QueryBuilders.matchQuery("workflowType", workflowName);
        List<HashMap> wraps = findAll(toIndexName(PENDING_WORKFLOWS), query, HashMap.class);
        Set<String> workflowIds = wraps.stream().map(map -> (String) map.get("workflowId")).collect(Collectors.toSet());
        logger.debug("getRunningWorkflowIds: result={}", workflowIds);

        return new ArrayList<>(workflowIds);
    }

    @Override
    public List<Workflow> getPendingWorkflowsByType(String workflowName) {
        logger.debug("getPendingWorkflowsByType: workflowName={}", workflowName);
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        List<Workflow> workflows = new LinkedList<Workflow>();
        List<String> wfIds = getRunningWorkflowIds(workflowName);
        for (String wfId : wfIds) {
            workflows.add(getWorkflow(wfId));
        }

        logger.debug("getPendingWorkflowsByType: result={}", toJson(workflows));
        return workflows;
    }

    @Override
    public long getPendingWorkflowCount(String workflowName) {
        logger.debug("getPendingWorkflowCount: workflowName={}", workflowName);

        String indexName = toIndexName(PENDING_WORKFLOWS);
        ensureIndexExists(indexName);

        QueryBuilder query = QueryBuilders.matchQuery("workflowType", workflowName);
        SearchResponse response = client.prepareSearch(indexName).setQuery(query).setSize(0).get();
        long result = response.getHits().getTotalHits();
        logger.debug("getPendingWorkflowCount: result={}", result);

        return result;
    }

    @Override
    public long getInProgressTaskCount(String taskDefName) {
        logger.debug("getInProgressTaskCount: taskDefName={}", taskDefName);

        String indexName = toIndexName(TASKS_IN_PROGRESS_STATUS);
        ensureIndexExists(indexName);

        QueryBuilder query = QueryBuilders.matchQuery("taskDefName", taskDefName);
        SearchResponse response = client.prepareSearch(indexName).setQuery(query).setSize(0).get();
        long result = response.getHits().getTotalHits();
        logger.debug("getInProgressTaskCount: result={}", result);

        return result;
    }

    @Override
    public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
        logger.debug("getWorkflowsByType: workflowName={}, startTime={}, endTime={}", workflowName, startTime, endTime);
        Preconditions.checkNotNull(workflowName, "workflowName cannot be null");
        Preconditions.checkNotNull(startTime, "startTime cannot be null");
        Preconditions.checkNotNull(endTime, "endTime cannot be null");

        List<Workflow> workflows = new LinkedList<Workflow>();

        List<String> dateStrs = dateStrBetweenDates(startTime, endTime);
        dateStrs.forEach(dateStr -> {
            //String key = nsKey(WORKFLOW_DEF_TO_WORKFLOWS, workflowName, dateStr);
            QueryBuilder typeQuery = QueryBuilders.matchQuery("workflowType", workflowName);
            QueryBuilder dateQuery = QueryBuilders.matchQuery("dateStr", dateStr);
            QueryBuilder finalQuery = QueryBuilders.boolQuery().must(typeQuery).must(dateQuery);
            List<HashMap> wraps = findAll(toIndexName(WORKFLOW_DEF_TO_WORKFLOWS), finalQuery, HashMap.class);
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

        logger.debug("getWorkflowsByType: result={}", toJson(workflows));
        return workflows;
    }

    @Override
    public List<Workflow> getWorkflowsByCorrelationId(String correlationId) {
        logger.debug("getWorkflowsByCorrelationId: correlationId={}", correlationId);

        Preconditions.checkNotNull(correlationId, "correlationId cannot be null");

        QueryBuilder query = QueryBuilders.matchQuery("correlationId", correlationId);
        List<HashMap> wraps = findAll(toIndexName(CORR_ID_TO_WORKFLOWS), query, HashMap.class);
        Set<String> workflowIds = wraps.stream().map(map -> (String) map.get("workflowId")).collect(Collectors.toSet());
        List<Workflow> workflows = workflowIds.stream().map(this::getWorkflow).collect(Collectors.toList());

        logger.debug("getWorkflowsByCorrelationId: result={}", toJson(workflows));
        return workflows;
    }

    @Override
    public boolean addEventExecution(EventExecution ee) {
        logger.debug("addEventExecution: ee={}", toJson(ee));
        try {
            String indexName = toIndexName(EVENT_EXECUTION);
            String typeName = toTypeName(ee.getName(), ee.getEvent(), ee.getMessageId());

            if (insert(indexName, typeName, ee.getId(), toMap(ee))) {
                indexer.add(ee);
                return true;
            }

            logger.debug("addEventExecution: done");
            return false;
        } catch (Exception ex) {
            logger.debug("addEventExecution: failed with {}", ex.getMessage());
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
        }
    }

    @Override
    public void updateEventExecution(EventExecution ee) {
        logger.debug("updateEventExecution: ee={}", toJson(ee));
        try {
            String indexName = toIndexName(EVENT_EXECUTION);
            String typeName = toTypeName(ee.getName(), ee.getEvent(), ee.getMessageId());

            upsert(indexName, typeName, ee.getId(), toMap(ee));

            indexer.add(ee);
            logger.debug("updateEventExecution: done", typeName);
        } catch (Exception ex) {
            logger.debug("updateEventExecution: failed with {}", ex.getMessage());
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
        }
    }

    @Override
    public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max) {
        logger.debug("getEventExecutions: eventHandlerName={}, eventName={}, messageId={}, max={}",
                eventHandlerName, eventName, messageId, max);
        try {
            String indexName = toIndexName(EVENT_EXECUTION);
            String typeName = toTypeName(eventHandlerName, eventName, messageId);

            List<EventExecution> executions = new LinkedList<>();
            for (int i = 0; i < max; i++) {
                String id = messageId + "_" + i;
                EventExecution ee = findOne(indexName, typeName, id, EventExecution.class);
                if (ee == null) {
                    break;
                }
                executions.add(ee);
            }

            logger.debug("getEventExecutions: result={}", toJson(executions));
            return executions;
        } catch (Exception ex) {
            logger.debug("getEventExecutions: failed with {}", ex.getMessage());
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, ex.getMessage(), ex);
        }
    }

    @Override
    public void addMessage(String queue, Message msg) {
        logger.debug("addMessage: queue={}, msg={}", queue, toJson(msg));
        indexer.addMessage(queue, msg);
    }

    @Override
    public void updateLastPoll(String queueName, String domain, String workerId) {
        logger.debug("updateLastPoll: queueName={}, domain={}, workerId={}", queueName, domain, workerId);
        Preconditions.checkNotNull(queueName, "queueName name cannot be null");
        PollData pollData = new PollData(queueName, domain, workerId, System.currentTimeMillis());

        String indexName = toIndexName(POLL_DATA);
        String typeName = toTypeName(queueName);
        String field = (domain == null) ? "DEFAULT" : domain;
        String id = toId(pollData.getQueueName(), field);

        upsert(indexName, typeName, id, toMap(pollData));

        logger.debug("updateLastPoll: done");
    }

    @Override
    public PollData getPollData(String queueName, String domain) {
        logger.debug("getPollData: queueName={}, domain={}", queueName, domain);
        Preconditions.checkNotNull(queueName, "queueName name cannot be null");

        String indexName = toIndexName(POLL_DATA);
        String typeName = toTypeName(queueName);
        String field = (domain == null) ? "DEFAULT" : domain;
        String id = toId(queueName, field);

        PollData pollData = findOne(indexName, typeName, id, PollData.class);

        logger.debug("getPollData: result={}", toJson(pollData));
        return null;
    }

    @Override
    public List<PollData> getPollData(String queueName) {
        logger.debug("getPollData: queueName={}", queueName);
        Preconditions.checkNotNull(queueName, "queueName name cannot be null");

        QueryBuilder query = QueryBuilders.matchQuery("queueName", queueName);
        List<PollData> pollData = findAll(toIndexName(POLL_DATA), query, PollData.class);

        logger.debug("getPollData: result={}", toJson(pollData));
        return pollData;
    }

    private String insertOrUpdateWorkflow(Workflow workflow, boolean update) {
        logger.debug("insertOrUpdateWorkflow: update={}, workflow={}", update, toJson(workflow));
        Preconditions.checkNotNull(workflow, "workflow object cannot be null");

        if (workflow.getStatus().isTerminal()) {
            workflow.setEndTime(System.currentTimeMillis());
        }
        List<Task> tasks = workflow.getTasks();
        workflow.setTasks(new LinkedList<>());

        String indexName = toIndexName(WORKFLOW);
        String typeName = toTypeName(workflow.getWorkflowType());
        String id = workflow.getWorkflowId();

        // Store the workflow object
        upsert(indexName, typeName, id, workflow);

        if (!update) {
            indexName = toIndexName(WORKFLOW_DEF_TO_WORKFLOWS);
            typeName = toTypeName(workflow.getWorkflowType());
            id = toId(workflow.getWorkflowType(), dateStr(workflow.getCreateTime()), workflow.getWorkflowId());

            // Add to list of workflows for a workflowdef
            Map<String, Object> payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
                    "workflowType", workflow.getWorkflowType(),
                    "dateStr", dateStr(workflow.getCreateTime()));
            insert(indexName, typeName, id, payload);

            // Add to list of workflows for a correlationId
            if (workflow.getCorrelationId() != null) {
                indexName = toIndexName(CORR_ID_TO_WORKFLOWS);
                typeName = toTypeName(workflow.getWorkflowType());
                id = toId(workflow.getCorrelationId(), workflow.getWorkflowId());

                payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
                        "correlationId", workflow.getCorrelationId());
                insert(indexName, typeName, id, payload);
            }
        }

        // Add or remove from the pending workflows
        if (workflow.getStatus().isTerminal()) {
            indexName = toIndexName(PENDING_WORKFLOWS);
            typeName = toTypeName(workflow.getWorkflowType());
            id = toId(workflow.getWorkflowType(), workflow.getWorkflowId());

            delete(indexName, typeName, id);
        } else {
            indexName = toIndexName(PENDING_WORKFLOWS);
            typeName = toTypeName(workflow.getWorkflowType());
            id = toId(workflow.getWorkflowType(), workflow.getWorkflowId());

            Map<String, Object> payload = ImmutableMap.of("workflowId", workflow.getWorkflowId(),
                    "workflowType", workflow.getWorkflowType());
            insert(indexName, typeName, id, payload);
        }

        workflow.setTasks(tasks);
        indexer.index(workflow);

        logger.debug("insertOrUpdateWorkflow: done={}", workflow.getWorkflowId());
        return workflow.getWorkflowId();
    }

    private static String dateStr(Long timeInMs) {
        Date date = new Date(timeInMs);
        return dateStr(date);
    }

    private static String dateStr(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(date);
    }

    private static List<String> dateStrBetweenDates(Long startdatems, Long enddatems) {
        logger.debug("dateStrBetweenDates: startdatems={}, enddatems={}", startdatems, enddatems);
        List<String> dates = new ArrayList<String>();
        Calendar calendar = new GregorianCalendar();
        Date startdate = new Date(startdatems);
        Date enddate = new Date(enddatems);
        calendar.setTime(startdate);
        while (calendar.getTime().before(enddate) || calendar.getTime().equals(enddate)) {
            Date result = calendar.getTime();
            dates.add(dateStr(result));
            calendar.add(Calendar.DATE, 1);
        }

        logger.debug("dateStrBetweenDates: result={}", dates);
        return dates;
    }
}

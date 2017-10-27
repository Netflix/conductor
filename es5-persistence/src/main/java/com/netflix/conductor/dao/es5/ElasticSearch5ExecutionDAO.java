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
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.*;

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
        List<Task> created = new LinkedList<Task>();

        for (Task task : tasks) {

            Preconditions.checkNotNull(task, "task object cannot be null");
            Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
            Preconditions.checkNotNull(task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
            Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");

            task.setScheduledTime(System.currentTimeMillis());

            String indexName = toIndexName(SCHEDULED_TASKS);
            String typeName = toTypeName(task.getTaskType());
            String taskKey = task.getReferenceTaskName() + "" + task.getRetryCount();
            String id = toId(task.getWorkflowInstanceId(), taskKey);

            if (exists(indexName, typeName, id)) {
                logger.debug("Task already scheduled, skipping the run " + task.getTaskId() + ", ref=" + task.getReferenceTaskName() + ", key=" + taskKey);
                continue;
            }
            // SCHEDULED_TASKS
            insert(indexName, typeName, id, wrap(task.getTaskId()));

            // WORKFLOW_TO_TASKS
            indexName = toIndexName(WORKFLOW_TO_TASKS);
            typeName = toTypeName(task.getTaskType());
            id = toId(task.getWorkflowInstanceId(), task.getTaskId());
            insert(indexName, typeName, id, wrap(task.getTaskId()));

            // IN_PROGRESS_TASKS
            indexName = toIndexName(IN_PROGRESS_TASKS);
            typeName = toTypeName(task.getTaskType());
            id = toId(task.getTaskDefName(), task.getTaskId());
            insert(indexName, typeName, id, wrap(task.getTaskId()));

            updateTask(task);
            created.add(task);
        }

        return created;
    }

    @Override // TODO Implement
    public void updateTask(Task task) {
        task.setUpdateTime(System.currentTimeMillis());
        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            task.setEndTime(System.currentTimeMillis());
        }

        TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());

        if(taskDef != null && taskDef.concurrencyLimit() > 0) {
            String indexName = toIndexName(TASKS_IN_PROGRESS_STATUS);
            String typeName = toTypeName(task.getTaskType());
            String id = toId(task.getTaskDefName(), task.getTaskId());

            if(task.getStatus() != null && task.getStatus().equals(Task.Status.IN_PROGRESS)) {
                //dynoClient.sadd(nsKey(TASKS_IN_PROGRESS_STATUS, task.getTaskDefName()), task.getTaskId());
                insert(indexName, typeName, id, wrap(task.getTaskId()));
            } else {
                //dynoClient.srem(nsKey(TASKS_IN_PROGRESS_STATUS, task.getTaskDefName()), task.getTaskId());
                delete(indexName, typeName, id);

                //String key = nsKey(TASK_LIMIT_BUCKET, task.getTaskDefName());
                //dynoClient.zrem(key, task.getTaskId());
                indexName = toIndexName(TASK_LIMIT_BUCKET);
                typeName = toTypeName(task.getTaskType());
                delete(indexName, typeName, id);
            }
        }

        String indexName = toIndexName(TASK);
        String typeName = toTypeName(task.getTaskType());
        String id = toId(task.getTaskId());
        insert(indexName, typeName, id, toMap(task));

        //dynoClient.set(nsKey(TASK, task.getTaskId()), toJson(task));
        if (task.getStatus() != null && task.getStatus().isTerminal()) {
            indexName = toIndexName(IN_PROGRESS_TASKS);
            typeName = toTypeName(task.getTaskType());
            id = toId(task.getTaskDefName(), task.getTaskId());
            delete(indexName, typeName, id);

            //dynoClient.srem(nsKey(IN_PROGRESS_TASKS, task.getTaskDefName()), task.getTaskId());
        }

        indexer.index(task);
    }

    @Override // TODO Implement
    public boolean exceedsInProgressLimit(Task task) {
        TaskDef taskDef = metadata.getTaskDef(task.getTaskDefName());
        if(taskDef == null) {
            return false;
        }
        int limit = taskDef.concurrencyLimit();
        if(limit <= 0) {
            return false;
        }

        long current = getInProgressTaskCount(task.getTaskDefName());
        if(current >= limit) {
            Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
            return true;
        }

        String indexName = toIndexName(TASK_LIMIT_BUCKET);
        String typeName = toTypeName(task.getTaskType());
        String id = toId(task.getTaskDefName(), task.getTaskId());

        double score = System.currentTimeMillis();
        String taskId = task.getTaskId();

        Map<String, Object> object = new HashMap<>();
        object.put("score", score);
        object.put("taskId", taskId);

        //String rateLimitKey = nsKey(TASK_LIMIT_BUCKET, task.getTaskDefName());
        // Adds all the specified members with the specified scores to the sorted set stored at key.
        // dynoClient.zaddnx(rateLimitKey, score, taskId); //NX - Don't update already existing elements. Always add new elements.
        insert(indexName, typeName, id, object);

        // Id prefix search as it is complex id: taskDefName + taskId
        QueryBuilder idQuery = QueryBuilders.wildcardQuery("_id", task.getTaskDefName() + "*");
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery("score").gte(0).lte(score + 1);
        QueryBuilder finalQuery = QueryBuilders.boolQuery().must(idQuery).must(rangeQuery);

        // Search ids where the score between 0 and score + 1 using limit
        //Set<String> ids = dynoClient.zrangeByScore(rateLimitKey, 0, score + 1, limit);
        List<String> ids = findIds(indexName, finalQuery, limit);

        boolean rateLimited = !ids.contains(taskId);
        if(rateLimited) {
            logger.info("Tak execution count limited. {}, limit {}, current {}", task.getTaskDefName(), limit, getInProgressTaskCount(task.getTaskDefName()));
            //Cleanup any items that are still present in the rate limit bucket but not in progress anymore!
            //String inProgressKey = nsKey(TASKS_IN_PROGRESS_STATUS, task.getTaskDefName());
            //ids.stream().filter(id -> !dynoClient.sismember(inProgressKey, id)).forEach(id2 -> dynoClient.zrem(rateLimitKey, id2));

            Monitors.recordTaskRateLimited(task.getTaskDefName(), limit);
        }
        return rateLimited;
    }

    @Override // TODO Implement
    public void addTaskExecLog(List<TaskExecLog> log) {
        indexer.add(log);
    }

    @Override
    public void updateTasks(List<Task> tasks) {
        for (Task task : tasks) {
            updateTask(task);
        }
    }

    @Override // TODO Implement
    public void removeTask(String taskId) {
        Task task = getTask(taskId);
        if(task == null) {
            logger.warn("No such Task by id {}", taskId);
            return;
        }
        String taskKey = task.getReferenceTaskName() + "" + task.getRetryCount();

//        dynoClient.hdel(nsKey(SCHEDULED_TASKS, task.getWorkflowInstanceId()), taskKey);
//        dynoClient.srem(nsKey(IN_PROGRESS_TASKS, task.getTaskDefName()), task.getTaskId());
//        dynoClient.srem(nsKey(WORKFLOW_TO_TASKS, task.getWorkflowInstanceId()), task.getTaskId());
//        dynoClient.srem(nsKey(TASKS_IN_PROGRESS_STATUS, task.getTaskDefName()), task.getTaskId());

        // dynoClient.del(nsKey(TASK, task.getTaskId()));
        delete(toIndexName(TASK), toTypeName(task.getTaskType()), taskId);

        // dynoClient.zrem(nsKey(TASK_LIMIT_BUCKET, task.getTaskDefName()), task.getTaskId());
        delete(toIndexName(TASK_LIMIT_BUCKET), toTypeName(task.getTaskType()), toId(task.getTaskDefName(), taskId));
    }

    @Override // TODO Implement
    public Task getTask(String taskId) {
        return null;
    }

    @Override // TODO Implement
    public List<Task> getTasks(List<String> taskIds) {
        return null;
    }

    @Override // TODO Implement
    public List<Task> getPendingTasksForTaskType(String taskType) {
        return null;
    }

    @Override // TODO Implement
    public List<Task> getTasksForWorkflow(String workflowId) {
        return null;
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

    @Override // TODO Implement
    public void removeWorkflow(String workflowId) {

    }

    @Override // TODO Implement
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {

    }

    @Override
    public Workflow getWorkflow(String workflowId) {
        return getWorkflow(workflowId, true);
    }

    @Override // TODO Implement
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        return null;
    }

    @Override // TODO Implement
    public List<String> getRunningWorkflowIds(String workflowName) {
        return null;
    }

    @Override // TODO Implement
    public List<Workflow> getPendingWorkflowsByType(String workflowName) {
        return null;
    }

    @Override // TODO Implement
    public long getPendingWorkflowCount(String workflowName) {
        return 0;
    }

    @Override // TODO Implement
    public long getInProgressTaskCount(String taskDefName) {
        return 0;
    }

    @Override // TODO Implement
    public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
        return null;
    }

    @Override // TODO Implement
    public List<Workflow> getWorkflowsByCorrelationId(String correlationId) {
        return null;
    }

    @Override // TODO Implement
    public boolean addEventExecution(EventExecution ee) {
        return false;
    }

    @Override // TODO Implement
    public void updateEventExecution(EventExecution ee) {
    }

    @Override // TODO Implement
    public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max) {
        return null;
    }

    @Override
    public void addMessage(String queue, Message msg) {
        indexer.addMessage(queue, msg);
    }

    @Override // TODO Implement
    public void updateLastPoll(String taskDefName, String domain, String workerId) {
    }

    @Override // TODO Implement
    public PollData getPollData(String taskDefName, String domain) {
        return null;
    }

    @Override // TODO Implement
    public List<PollData> getPollData(String taskDefName) {
        return null;
    }

    private String insertOrUpdateWorkflow(Workflow workflow, boolean update) {
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
            insert(indexName, typeName, id, wrap(workflow.getWorkflowId()));

            // Add to list of workflows for a correlationId
            if (workflow.getCorrelationId() != null) {
                indexName = toIndexName(CORR_ID_TO_WORKFLOWS);
                typeName = toTypeName(workflow.getWorkflowType());
                id = toId(workflow.getCorrelationId(), workflow.getWorkflowId());

                insert(indexName, typeName, id, wrap(workflow.getWorkflowId()));
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

            insert(indexName, typeName, id, wrap(workflow.getWorkflowId()));
        }

        workflow.setTasks(tasks);
        indexer.index(workflow);

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
        return dates;
    }
}

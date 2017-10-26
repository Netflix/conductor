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
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5ExecutionDAO extends ElasticSearch5BaseDAO implements ExecutionDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5ExecutionDAO.class);
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
        return null;
    }

    @Override
    public List<Task> getTasks(String taskType, String startKey, int count) {
        return null;
    }

    @Override
    public List<Task> createTasks(List<Task> tasks) {
        return null;
    }

    @Override
    public void updateTask(Task task) {

    }

    @Override
    public boolean exceedsInProgressLimit(Task task) {
        return false;
    }

    @Override
    public void updateTasks(List<Task> tasks) {

    }

    @Override
    public void addTaskExecLog(List<TaskExecLog> log) {

    }

    @Override
    public void removeTask(String taskId) {

    }

    @Override
    public Task getTask(String taskId) {
        return null;
    }

    @Override
    public List<Task> getTasks(List<String> taskIds) {
        return null;
    }

    @Override
    public List<Task> getPendingTasksForTaskType(String taskType) {
        return null;
    }

    @Override
    public List<Task> getTasksForWorkflow(String workflowId) {
        return null;
    }

    @Override
    public String createWorkflow(Workflow workflow) {
        return null;
    }

    @Override
    public String updateWorkflow(Workflow workflow) {
        return null;
    }

    @Override
    public void removeWorkflow(String workflowId) {

    }

    @Override
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {

    }

    @Override
    public Workflow getWorkflow(String workflowId) {
        return null;
    }

    @Override
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        return null;
    }

    @Override
    public List<String> getRunningWorkflowIds(String workflowName) {
        return null;
    }

    @Override
    public List<Workflow> getPendingWorkflowsByType(String workflowName) {
        return null;
    }

    @Override
    public long getPendingWorkflowCount(String workflowName) {
        return 0;
    }

    @Override
    public long getInProgressTaskCount(String taskDefName) {
        return 0;
    }

    @Override
    public List<Workflow> getWorkflowsByType(String workflowName, Long startTime, Long endTime) {
        return null;
    }

    @Override
    public List<Workflow> getWorkflowsByCorrelationId(String correlationId) {
        return null;
    }

    @Override
    public boolean addEventExecution(EventExecution ee) {
        return false;
    }

    @Override
    public void updateEventExecution(EventExecution ee) {

    }

    @Override
    public List<EventExecution> getEventExecutions(String eventHandlerName, String eventName, String messageId, int max) {
        return null;
    }

    @Override
    public void addMessage(String queue, Message msg) {

    }

    @Override
    public void updateLastPoll(String taskDefName, String domain, String workerId) {

    }

    @Override
    public PollData getPollData(String taskDefName, String domain) {
        return null;
    }

    @Override
    public List<PollData> getPollData(String taskDefName) {
        return null;
    }
}

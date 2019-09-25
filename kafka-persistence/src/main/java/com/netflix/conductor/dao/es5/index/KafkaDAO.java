/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.netflix.conductor.dao.es5.index;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.dao.kafka.index.utils.RecordTypeConstants;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author Manan
 */
@Trace
@Singleton
public class KafkaDAO implements IndexDAO {

    private ProducerDAO producerDAO;

    public KafkaDAO(KafkaProducer producer) {
        this.producerDAO = producer;
    }

    @Override
    public void setup() {

    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        WorkflowSummary summary = new WorkflowSummary(workflow);
        producerDAO.send(RecordTypeConstants.WORKFLOW_DOC_TYPE, summary);
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
        return null;
    }

    @Override
    public void indexTask(Task task) {
        TaskSummary summary = new TaskSummary(task);
        producerDAO.send(RecordTypeConstants.TASK_DOC_TYPE, summary);
    }

    @Override
    public CompletableFuture<Void> asyncIndexTask(Task task) {
        return null;
    }

    @Override
    public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
        return null;
    }

    @Override
    public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
        return null;
    }

    @Override
    public void removeWorkflow(String workflowId) {

    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        return null;
    }

    @Override
    public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {

    }

    @Override
    public CompletableFuture<Void> asyncUpdateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        return null;
    }

    @Override
    public String get(String workflowInstanceId, String key) {
        return null;
    }

    @Override
    public void addMessage(String queue, Message message) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("messageId", message.getId());
        doc.put("payload", message.getPayload());
        doc.put("queue", queue);
        doc.put("created", System.currentTimeMillis());

        producerDAO.send(RecordTypeConstants.MSG_DOC_TYPE, doc);
    }

    @Override
    public List<Message> getMessages(String queue) {
        return null;
    }

    @Override
    public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
        return null;
    }

    @Override
    public List<String> searchRecentRunningWorkflows(int lastModifiedHoursAgoFrom, int lastModifiedHoursAgoTo) {
        return null;
    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
        String id = eventExecution.getName() + "." + eventExecution.getEvent() + "." + eventExecution.getMessageId() + "." + eventExecution.getId();
        producerDAO.send( RecordTypeConstants.EVENT_DOC_TYPE, id);
    }

    @Override
    public List<EventExecution> getEventExecutions(String event) {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
        return null;
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs.isEmpty()) {
            return;
        }
        taskExecLogs.forEach(log -> producerDAO.send(RecordTypeConstants.LOG_DOC_TYPE , taskExecLogs));
    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
        return null;
    }

    @Override
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        return null;
    }

}

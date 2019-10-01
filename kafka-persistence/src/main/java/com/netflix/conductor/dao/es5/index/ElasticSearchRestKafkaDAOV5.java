package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.utils.DocumentTypes;
import com.netflix.conductor.dao.kafka.index.utils.OperationTypes;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.elasticsearch.client.RestClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


@Trace
@Singleton
public class ElasticSearchRestKafkaDAOV5 extends ElasticSearchRestDAOV5 {

    private ProducerDAO producerDAO;

    @Inject
    public ElasticSearchRestKafkaDAOV5(RestClient lowLevelRestClient, ElasticSearchConfiguration config, ObjectMapper objectMapper, ProducerDAO kafkaProducer) {
        super(lowLevelRestClient, config, objectMapper);
        this.producerDAO = kafkaProducer;
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
        WorkflowSummary summary = new WorkflowSummary(workflow);
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.WORKFLOW_DOC_TYPE, summary);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> asyncIndexTask(Task task) {
        TaskSummary summary = new TaskSummary(task);
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.TASK_DOC_TYPE, summary);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addMessage(String queue, Message message) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("messageId", message.getId());
        doc.put("payload", message.getPayload());
        doc.put("queue", queue);
        doc.put("created", System.currentTimeMillis());

        producerDAO.send(OperationTypes.CREATE, DocumentTypes.MSG_DOC_TYPE, doc);

    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution)  {
        String id = eventExecution.getName() + "." + eventExecution.getEvent() + "." + eventExecution.getMessageId() + "." + eventExecution.getId();
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.EVENT_DOC_TYPE, id);
        return CompletableFuture.completedFuture(null);

    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> taskExecLogs)  {
        if (taskExecLogs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        taskExecLogs.forEach(log -> producerDAO.send(OperationTypes.CREATE, DocumentTypes.LOG_DOC_TYPE , taskExecLogs));
        return CompletableFuture.completedFuture(null);

    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        producerDAO.send(OperationTypes.DELETE, DocumentTypes.WORKFLOW_DOC_TYPE, workflowId);
        return CompletableFuture.completedFuture(null);

    }



}

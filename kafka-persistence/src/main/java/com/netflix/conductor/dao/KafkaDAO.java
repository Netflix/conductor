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
package com.netflix.conductor.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestDAOV5;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.kafka.index.utils.DocumentTypes;
import com.netflix.conductor.kafka.index.utils.OperationTypes;
import com.netflix.conductor.metrics.Monitors;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @author Manan
 */
@Trace
@Singleton
public class KafkaDAO extends ElasticSearchRestDAOV5 {

    private final ThreadPoolExecutor executorService;
    private ProducerDAO producerDAO;
    private static Logger logger = LoggerFactory.getLogger(KafkaDAO.class);
    private Histogram.Timer latencyTimer;
    private Histogram kafkaPublishLatency;

    @Inject
    public KafkaDAO(KafkaProducer producer, RestClient lowLevelRestClient, ElasticSearchConfiguration config, ObjectMapper objectMapper) {
        super(lowLevelRestClient, config, objectMapper);
        this.producerDAO = producer;

        // Set up a workerpool for performing async operations.
        int corePoolSize = 6;
        int maximumPoolSize = config.getAsyncMaxPoolSize();
        long keepAliveTime = 1L;
        int workerQueueSize = config.getAsyncWorkerQueueSize();
        this.executorService = new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(workerQueueSize),
                (runnable, executor) -> {
                    logger.warn("Request  {} to async dao discarded in executor {}", runnable, executor);
                });
        kafkaPublishLatency = Histogram.build()
                .name("kafka_publish_latency_seconds")
                .labelNames("operationType", "documentType")
                .help("Kafka Publish Latency").register(CollectorRegistry.defaultRegistry);
    }

    @Override
    public void setup() {

    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        latencyTimer = kafkaPublishLatency.labels(OperationTypes.CREATE, DocumentTypes.WORKFLOW_DOC_TYPE).startTimer();
        WorkflowSummary summary = new WorkflowSummary(workflow);
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.WORKFLOW_DOC_TYPE, summary);
        latencyTimer.observeDuration();
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
        return CompletableFuture.runAsync(() -> indexWorkflow(workflow), executorService);
    }

    @Override
    public void indexTask(Task task) {
        latencyTimer = kafkaPublishLatency.labels(OperationTypes.CREATE, DocumentTypes.TASK_DOC_TYPE).startTimer();
        TaskSummary summary = new TaskSummary(task);
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.TASK_DOC_TYPE, summary);
        latencyTimer.observeDuration();
    }

    @Override
    public CompletableFuture<Void> asyncIndexTask(Task task) {
        return CompletableFuture.runAsync(() -> indexTask(task), executorService);
    }

    @Override
    public void removeWorkflow(String workflowId) {
        latencyTimer = kafkaPublishLatency.labels(OperationTypes.DELETE, DocumentTypes.WORKFLOW_DOC_TYPE).startTimer();
        producerDAO.send(OperationTypes.DELETE, DocumentTypes.WORKFLOW_DOC_TYPE, workflowId);
        latencyTimer.observeDuration();
    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        return CompletableFuture.runAsync(() -> removeWorkflow(workflowId), executorService);
    }

    @Override
    public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        latencyTimer = kafkaPublishLatency.labels(OperationTypes.UPDATE, DocumentTypes.WORKFLOW_DOC_TYPE).startTimer();
        values[1] = workflowInstanceId;
        producerDAO.send(OperationTypes.UPDATE, DocumentTypes.WORKFLOW_DOC_TYPE, values);
        latencyTimer.observeDuration();
    }

    @Override
    public CompletableFuture<Void> asyncUpdateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        return CompletableFuture.runAsync(() -> updateWorkflow(workflowInstanceId, keys, values), executorService);
    }

    @Override
    public void addMessage(String queue, Message message) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("messageId", message.getId());
        doc.put("payload", message.getPayload());
        doc.put("queue", queue);
        doc.put("created", System.currentTimeMillis());

        long start = System.currentTimeMillis();
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.MSG_DOC_TYPE, doc);
        Monitors.getTimer(Monitors.classQualifier, "kafka_produce_time", DocumentTypes.MSG_DOC_TYPE, OperationTypes.CREATE).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);

    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
        latencyTimer = kafkaPublishLatency.labels(OperationTypes.CREATE, DocumentTypes.EVENT_DOC_TYPE).startTimer();
        String id = eventExecution.getName() + "." + eventExecution.getEvent() + "." + eventExecution.getMessageId() + "." + eventExecution.getId();
        producerDAO.send(OperationTypes.CREATE, DocumentTypes.EVENT_DOC_TYPE, id);
        latencyTimer.observeDuration();
    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
        return CompletableFuture.runAsync(() -> addEventExecution(eventExecution), executorService);
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs.isEmpty()) {
            return;
        }
        latencyTimer = kafkaPublishLatency.labels(OperationTypes.CREATE, DocumentTypes.LOG_DOC_TYPE).startTimer();
        taskExecLogs.forEach(log -> producerDAO.send(OperationTypes.CREATE, DocumentTypes.LOG_DOC_TYPE , taskExecLogs));
        latencyTimer.observeDuration();
    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
        return CompletableFuture.runAsync(() -> addTaskExecutionLogs(logs), executorService);
    }
}
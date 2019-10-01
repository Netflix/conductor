package com.netflix.conductor.dao.kafka.index.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Singleton;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.kafka.index.constants.ConsumerConstants;
import com.netflix.conductor.dao.kafka.index.data.Record;
import com.netflix.conductor.dao.kafka.index.mapper.MapperFactory;
import com.netflix.conductor.dao.kafka.index.utils.DocumentTypes;
import com.netflix.conductor.dao.kafka.index.utils.OperationTypes;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.*;


@Singleton
@Trace
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private ObjectMapper om = MapperFactory.getObjectMapper();

    private String requestTimeoutConfig;
    private int pollInterval;
    private Consumer consumer;

    @Inject private String topic;
    ScheduledExecutorService scheduler;
    private IndexDAO indexDAO;
    private ElasticSearchConfiguration elasticSearchConfiguration;

    @Inject
    public KafkaConsumer(ElasticSearchConfiguration elasticSearchConfiguration, IndexDAO indexDAO) {
        this.elasticSearchConfiguration = elasticSearchConfiguration;
        this.indexDAO = indexDAO;
    }

    public void init() {
        this.requestTimeoutConfig = elasticSearchConfiguration.getProperty(ConsumerConstants.KAFKA_REQUEST_TIMEOUT_MS, ConsumerConstants.DEFAULT_REQUEST_TIMEOUT);
        String requestTimeoutMs = requestTimeoutConfig;

        this.pollInterval = elasticSearchConfiguration.getIntProperty(ConsumerConstants.KAFKA_CONSUMER_POLL_INTERVAL, ConsumerConstants.CONSUMER_DEFAULT_POLL_INTERVAL);
        this.topic = elasticSearchConfiguration.getProperty(ConsumerConstants.KAFKA_CONSUMER_TOPIC, ConsumerConstants.CONSUMER_DEFAULT_TOPIC);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, elasticSearchConfiguration.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConstants.DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConstants.STRING_DESERIALIZER);
        consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConsumerConstants.STRING_DESERIALIZER);
        consumerConfig.put("group.id",  "_group");
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(consumerConfig);

        consumer.subscribe(Collections.singleton(this.topic));

        scheduler = Executors.newScheduledThreadPool(elasticSearchConfiguration.getKafkaConsumerPoolSize());
        try {
            scheduler.scheduleAtFixedRate(() -> consume(), 0, 10, TimeUnit.MILLISECONDS);
        } catch(RejectedExecutionException e) {
            logger.error("Task Rejected in scheduler Exception {}", e);
        }
    }

    public void consume() {
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(this.pollInterval));
            records.forEach(record -> consumeData(record.value()));
        } catch (KafkaException e) {
            logger.error("kafka KafkaConsumer message polling failed.", e);
        }
    }

    public void consumeData(String data) {
        try {
            Record d = om.readValue(data, Record.class);
            long start = System.currentTimeMillis();
            switch (d.getDocumentType()) {
                case DocumentTypes.WORKFLOW_DOC_TYPE:
                    if (d.getOperationType().equals(OperationTypes.DELETE)) {
                        removeWorkflow(d.getPayload());
                    } else {
                        consumeWorkflow(d.getPayload(), om.readTree(data).get("workflowId").asText());
                    }
                    break;
                case DocumentTypes.TASK_DOC_TYPE:
                    consumeTask(d.getPayload(), om.readTree(data).get("taskId").asText());
                    break;
                case DocumentTypes.LOG_DOC_TYPE:
                    consumeTaskLog(d.getPayload());
                    break;
                case DocumentTypes.EVENT_DOC_TYPE:
                    consumeEventExecution(d.getPayload());
                    break;
                case DocumentTypes.MSG_DOC_TYPE:
                    consumeMessage(d.getPayload(), om.readTree(data).get("queue").asText());
                    break;
                default:
                    break;
            }
            Monitors.getTimer(Monitors.classQualifier, "elastic_search_index_time", "elastic_search_index_time").record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to consume from kafka - unknown exception:", e);
        }
    }

    private void removeWorkflow(Object data) {
        try {
            indexDAO.removeWorkflow(om.writeValueAsString(data));
        } catch (Exception e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to remove workflow: {}", data, e);
        }
    }

    private void consumeWorkflow(Object data, String id) {
        try {
            byte[] payload = om.writeValueAsBytes(data);
            WorkflowSummary workflowSummary  = om.readValue(new String(payload), WorkflowSummary.class);
            indexDAO.asyncIndexWorkflowSummary(workflowSummary);
        } catch (Exception e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to index workflow: {}", id, e);
        }
    }


    private void consumeTask(Object data, String id) {
        try {
            byte[] payload = om.writeValueAsBytes(data);
            TaskSummary taskSummary  = om.readValue(new String(payload), TaskSummary.class);
            indexDAO.asyncIndexTaskSummary(taskSummary);
        } catch (Exception e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to index task: {}", id, e);
        }
    }

    private void consumeTaskLog(Object data) {
        try {
            byte[] payload = om.writeValueAsBytes(data);
            TaskExecLog log  = om.readValue(new String(payload), TaskExecLog.class);
            indexDAO.asyncAddTaskExecutionLogs(Arrays.asList(log));
        } catch (Exception e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to index task log: {}", data, e);
        }
    }

    private void consumeEventExecution(Object data) {
        try {
            byte[] payload = om.writeValueAsBytes(data);
            EventExecution log  = om.readValue(new String(payload), EventExecution.class);
            indexDAO.asyncAddEventExecution(log);
        } catch (Exception e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to index event execution: {}", data, e);
        }
    }

    private void consumeMessage(Object data, String queue) {
        try {
            byte[] payload = om.writeValueAsBytes(data);
            Message message  = om.readValue(new String(payload), Message.class);
            indexDAO.addMessage(queue, message);
        } catch (Exception e) {
            // JSON is not formatted. Workflow details in UI won't be available.
            logger.error("Failed to index message: {}", data, e);
        }
    }

}

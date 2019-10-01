package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.kafka.index.constants.ProducerConstants;
import com.netflix.conductor.dao.kafka.index.mapper.MapperFactory;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.dao.kafka.index.utils.DocumentTypes;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.Client;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.netflix.conductor.core.config.Configuration.KAFKA_INDEX_ENABLE;
import static com.netflix.conductor.dao.kafka.index.constants.ProducerConstants.KAFKA_PRODUCER_TOPIC;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_ASYNC_DAO_MAX_POOL_SIZE;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_ASYNC_DAO_WORKER_QUEUE_SIZE;
import static org.awaitility.Awaitility.await;

public class ElasticSearchKafkaDAOV5Test {

    private static ElasticSearchDAOV5 indexDAO;
    private static Client restClient;
    private static ElasticSearchConfiguration configuration;
    private static ObjectMapper objectMapper;
    private static KafkaProducer producer;
    private static KafkaConsumer<String, String> consumer;


    @BeforeClass
    public static void start() throws Exception {
        restClient = Mockito.mock(Client.class);
        System.setProperty(ELASTIC_SEARCH_ASYNC_DAO_MAX_POOL_SIZE, "6");
        System.setProperty(ELASTIC_SEARCH_ASYNC_DAO_WORKER_QUEUE_SIZE, "1");
        System.setProperty(KAFKA_PRODUCER_TOPIC, "local");
        System.setProperty(KAFKA_INDEX_ENABLE, "true");

        configuration = new SystemPropertiesElasticSearchConfiguration();
        objectMapper = MapperFactory.getObjectMapper();
        producer = new KafkaProducer(configuration);
        indexDAO = new ElasticSearchKafkaDAOV5(restClient, configuration, objectMapper, producer);
        consumer = new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.DEFAULT_BOOTSTRAP_SERVERS_CONFIG,
                        ConsumerConfig.GROUP_ID_CONFIG, "conductor-" + UUID.randomUUID(),
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                        ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        consumer.subscribe(Arrays.asList("local"));
    }

    @AfterClass
    public static void close() {
        consumer.unsubscribe();
    }

    @Test
    public void testIndexWorkflow() {
        Workflow workflow = new Workflow();
        String workflowId = "search-workflow-id";
        workflow.setWorkflowId(workflowId);
        indexDAO.indexWorkflow(workflow);
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(DocumentTypes.WORKFLOW_DOC_TYPE)));
        });
    }

    @Test
    public void testIndexTask() {
        Task task = new Task();
        indexDAO.indexTask(task);
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(DocumentTypes.TASK_DOC_TYPE)));
        });    }

    @Test
    public void testAddMessage() {
        Message message = new Message();
        indexDAO.addMessage("queue", message);
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(DocumentTypes.MSG_DOC_TYPE)));
        });    }

    @Test
    public void testAddEventExecution() {
        EventExecution eventExecution = new EventExecution();
        indexDAO.addEventExecution(eventExecution);
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(DocumentTypes.EVENT_DOC_TYPE)));
        });    }

    @Test
    public void testAddTaskExecutionLogs() {
        TaskExecLog taskExecLog = new TaskExecLog();
        indexDAO.addTaskExecutionLogs(Arrays.asList(taskExecLog));
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(DocumentTypes.LOG_DOC_TYPE)));
        });    }
}

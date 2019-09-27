package com.netflix.conductor.dao.kafka;

import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.kafka.index.KafkaConfiguration;
import com.netflix.conductor.dao.kafka.index.KafkaDAO;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.dao.kafka.index.utils.RecordTypeConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.netflix.conductor.core.config.Configuration.KAFKA_INDEX_ENABLE;
import static com.netflix.conductor.dao.kafka.index.constants.ProducerConstants.KAFKA_PRODUCER_TOPIC;
import static org.awaitility.Awaitility.await;

@Ignore
public class KafkaDAOTest {

    private static IndexDAO indexDAO;
    private static Configuration configuration;
    private static KafkaProducer producer;
    private static KafkaConsumer<String, String> consumer;
    private static KafkaContainer kafka;


    @BeforeClass
    public static void start() throws Exception {
        System.setProperty(KAFKA_PRODUCER_TOPIC, "local");
        System.setProperty(KAFKA_INDEX_ENABLE, "true");
        kafka = new KafkaContainer();
        kafka.start();
        System.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        configuration = new KafkaConfiguration();
        producer = new KafkaProducer(configuration);
        indexDAO = new KafkaDAO(producer);
        consumer = new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "conductor-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                        ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        consumer.subscribe(Arrays.asList("local"));
    }

    @AfterClass
    public static void close() {
        kafka.close();
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
            records.forEach(record -> Assert.assertTrue(record.value().contains(RecordTypeConstants.WORKFLOW_DOC_TYPE)));
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
            records.forEach(record -> Assert.assertTrue(record.value().contains(RecordTypeConstants.TASK_DOC_TYPE)));
        });    }

    @Test
    public void testAddMessage() {
        Message message = new Message();
        indexDAO.addMessage("queue", message);
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(RecordTypeConstants.MSG_DOC_TYPE)));
        });    }

    @Test
    public void testAddEventExecution() {
        EventExecution eventExecution = new EventExecution();
        indexDAO.addEventExecution(eventExecution);
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(RecordTypeConstants.EVENT_DOC_TYPE)));
        });    }

    @Test
    public void testAddTaskExecutionLogs() {
        TaskExecLog taskExecLog = new TaskExecLog();
        indexDAO.addTaskExecutionLogs(Arrays.asList(taskExecLog));
        await()
                .atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Assert.assertEquals(records.count(), 1);
            records.forEach(record -> Assert.assertTrue(record.value().contains(RecordTypeConstants.LOG_DOC_TYPE)));
        });
    }
}

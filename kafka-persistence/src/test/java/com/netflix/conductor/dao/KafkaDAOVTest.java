package com.netflix.conductor.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.kafka.index.utils.DocumentTypes;
import com.netflix.conductor.kafka.index.utils.OperationTypes;
import org.elasticsearch.client.RestClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

public class KafkaDAOVTest {

    private static KafkaDAO indexDAO;
    private static KafkaProducer producerDAO;
    private static RestClient lowLevelRestClient;
    private static ElasticSearchConfiguration config;
    private static ObjectMapper objectMapper;

    @BeforeClass
    public static void start() {
        producerDAO = Mockito.mock(KafkaProducer.class);
        lowLevelRestClient = Mockito.mock(RestClient.class);
        config = Mockito.mock(ElasticSearchConfiguration.class);
        objectMapper = Mockito.mock(ObjectMapper.class);
        Mockito.doReturn(10).when(config).getAsyncMaxPoolSize();
        Mockito.doReturn(1).when(config).getAsyncWorkerQueueSize();
        indexDAO = new KafkaDAO(producerDAO, lowLevelRestClient, config, objectMapper);
        Mockito.doNothing().when(producerDAO).send(any(String.class), any(String.class), any(Object.class));
    }

    @Test
    public void testIndexWorkflow() {
        Workflow workflow = new Workflow();
        indexDAO.indexWorkflow(workflow);
        Mockito.verify(producerDAO).send(eq(OperationTypes.CREATE), eq(DocumentTypes.WORKFLOW_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testIndexTask() {
        Task task = new Task();
        indexDAO.indexTask(task);
        Mockito.verify(producerDAO).send(eq(OperationTypes.CREATE), eq(DocumentTypes.TASK_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testAddMessage() {
        Message message = new Message();
        indexDAO.addMessage("queue", message);
        Mockito.verify(producerDAO).send(eq(OperationTypes.CREATE), eq(DocumentTypes.MSG_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testAddEventExecution() {
        EventExecution eventExecution = new EventExecution();
        indexDAO.addEventExecution(eventExecution);
        Mockito.verify(producerDAO).send(eq(OperationTypes.CREATE), eq(DocumentTypes.EVENT_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testAddTaskExecutionLogs() {
        TaskExecLog taskExecLog = new TaskExecLog();
        indexDAO.addTaskExecutionLogs(Arrays.asList(taskExecLog));
        Mockito.verify(producerDAO).send(eq(OperationTypes.CREATE), eq(DocumentTypes.LOG_DOC_TYPE), any(Object.class));
    }
}

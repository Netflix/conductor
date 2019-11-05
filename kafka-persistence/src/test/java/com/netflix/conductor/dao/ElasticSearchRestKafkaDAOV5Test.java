package com.netflix.conductor.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.kafka.index.utils.DocumentTypes;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;
import org.elasticsearch.client.RestClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_ASYNC_DAO_MAX_POOL_SIZE;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_ASYNC_DAO_WORKER_QUEUE_SIZE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

public class ElasticSearchRestKafkaDAOV5Test {

    private static ElasticSearchRestKafkaDAOV5 indexDAO;
    private static RestClient restClient;
    private static ElasticSearchConfiguration configuration;
    private static ObjectMapper objectMapper;
    private static ProducerDAO producerDAO;



    @BeforeClass
    public static void start() throws Exception {
        restClient = Mockito.mock(RestClient.class);
        System.setProperty(ELASTIC_SEARCH_ASYNC_DAO_MAX_POOL_SIZE, "1");
        System.setProperty(ELASTIC_SEARCH_ASYNC_DAO_WORKER_QUEUE_SIZE, "1");

        configuration = new SystemPropertiesElasticSearchConfiguration();
        objectMapper = Mockito.mock(ObjectMapper.class);
        producerDAO = Mockito.mock(ProducerDAO.class);
        indexDAO = new ElasticSearchRestKafkaDAOV5(restClient, configuration, objectMapper, producerDAO);
        Mockito.doNothing().when(producerDAO).send(any(String.class), any(String.class), any(Object.class));
    }

    @Test
    public void testIndexWorkflow() {
        Workflow workflow = new Workflow();
        indexDAO.indexWorkflow(workflow);
        Mockito.verify(producerDAO).send(any(String.class), eq(DocumentTypes.WORKFLOW_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testIndexTask() {
        Task task = new Task();
        indexDAO.indexTask(task);
        Mockito.verify(producerDAO).send(any(String.class), eq(DocumentTypes.TASK_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testAddMessage() {
        Message message = new Message();
        indexDAO.addMessage("queue", message);
        Mockito.verify(producerDAO).send(any(String.class), eq(DocumentTypes.MSG_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testAddEventExecution() {
        EventExecution eventExecution = new EventExecution();
        indexDAO.addEventExecution(eventExecution);
        Mockito.verify(producerDAO).send(any(String.class), eq(DocumentTypes.EVENT_DOC_TYPE), any(Object.class));
    }

    @Test
    public void testAddTaskExecutionLogs() {
        TaskExecLog taskExecLog = new TaskExecLog();
        indexDAO.addTaskExecutionLogs(Arrays.asList(taskExecLog));
        Mockito.verify(producerDAO).send(any(String.class), eq(DocumentTypes.LOG_DOC_TYPE), any(Object.class));
    }
}

package com.netflix.conductor.consumer;

import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.exception.ObfuscationServiceException;
import com.netflix.conductor.service.ObfuscationService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class WorkflowObfuscationQueueConsumerTest {

    private ObfuscationService obfuscationService;
    private QueueDAO queueDAO;
    private Configuration configuration;
    private WorkflowObfuscationQueueConsumer consumer;
    private static String workflowId = "f580ba3a-7c03-4db3-aab0-7d35f28aeb14";
    private static String obfuscationQueue = "_obfuscationQueue";

    @Before
    public void setup() {
        queueDAO = Mockito.mock(QueueDAO.class);
        obfuscationService = Mockito.mock(ObfuscationService.class);
        configuration = Mockito.mock(Configuration.class);

        Mockito.when(configuration.getBooleanProperty("workflow.obfuscation.enabled", false)).thenReturn(true);
        Mockito.when(configuration.getIntProperty("workflow.obfuscation.consumer.thread.count", 5)).thenReturn(1);
        Mockito.when(configuration.getProperty("workflow.obfuscation.queue.name", "_obfuscationQueue")).thenReturn("_obfuscationQueue");
        Mockito.when(queueDAO.pop("_obfuscationQueue", 2, 2000)).thenReturn(Collections.singletonList(workflowId));
    }

    @Test
    public void should_not_process_workflow_obfuscations_if_coordinator_is_disabled() throws InterruptedException {
        Mockito.when(configuration.getBooleanProperty("workflow.obfuscation.enabled", false)).thenReturn(false);
        consumer = new WorkflowObfuscationQueueConsumer(configuration, obfuscationService, queueDAO);

        Thread.sleep(600);

        Mockito.verify(obfuscationService, Mockito.times(0)).obfuscateFields(workflowId);
        Mockito.verify(queueDAO, Mockito.times(0)).remove(obfuscationQueue, workflowId);
    }

    @Test
    public void should_process_workflow_obfuscations() throws InterruptedException {
        consumer = new WorkflowObfuscationQueueConsumer(configuration, obfuscationService, queueDAO);

        Thread.sleep(600);

        Mockito.verify(obfuscationService, Mockito.times(1)).obfuscateFields(workflowId);
        Mockito.verify(queueDAO, Mockito.times(1)).remove(obfuscationQueue, workflowId);
    }

    @Test
    public void should_process_workflow_obfuscations_on_service_exceptions() throws InterruptedException {
        Mockito.doThrow(new ObfuscationServiceException("error")).when(obfuscationService).obfuscateFields(workflowId);
        consumer = new WorkflowObfuscationQueueConsumer(configuration, obfuscationService, queueDAO);

        Thread.sleep(600);

        Mockito.verify(obfuscationService, Mockito.times(1)).obfuscateFields(workflowId);
        Mockito.verify(queueDAO, Mockito.times(1)).remove(obfuscationQueue, workflowId);
    }

    @Test
    public void should_not_process_workflow_obfuscations_unexpected_exceptions() throws InterruptedException {
        Mockito.doThrow(new RuntimeException()).when(obfuscationService).obfuscateFields(workflowId);
        consumer = new WorkflowObfuscationQueueConsumer(configuration, obfuscationService, queueDAO);

        Thread.sleep(600);

        Mockito.verify(obfuscationService, Mockito.times(1)).obfuscateFields(workflowId);
        Mockito.verify(queueDAO, Mockito.times(0)).remove(obfuscationQueue, workflowId);
    }

}

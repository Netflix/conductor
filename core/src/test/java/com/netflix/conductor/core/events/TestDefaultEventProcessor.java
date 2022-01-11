/*
 * Copyright 2021 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.events;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.common.metadata.events.EventHandler.Action.Type;
import com.netflix.conductor.common.metadata.events.EventHandler.StartWorkflow;
import com.netflix.conductor.common.metadata.events.EventHandler.TaskDetails;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.exception.ApplicationException;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.evaluators.Evaluator;
import com.netflix.conductor.core.execution.evaluators.JavascriptEvaluator;
import com.netflix.conductor.core.utils.JsonUtils;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ContextConfiguration(
        classes = {
            TestObjectMapperConfiguration.class,
            TestDefaultEventProcessor.TestConfiguration.class
        })
@RunWith(SpringRunner.class)
public class TestDefaultEventProcessor {

    private String event;
    private ObservableQueue queue;
    private MetadataService metadataService;
    private ExecutionService executionService;
    private WorkflowExecutor workflowExecutor;
    private SimpleActionProcessor actionProcessor;
    private EventQueues eventQueues;
    private ParametersUtils parametersUtils;
    private JsonUtils jsonUtils;
    private ConductorProperties properties;
    private Message message;

    @Autowired private Map<String, Evaluator> evaluators;

    @Autowired private ObjectMapper objectMapper;

    @Configuration
    @ComponentScan(basePackageClasses = {Evaluator.class}) // load all Evaluator beans
    public static class TestConfiguration {}

    @Before
    public void setup() {
        event = "sqs:arn:account090:sqstest1";
        String queueURI = "arn:account090:sqstest1";

        metadataService = mock(MetadataService.class);
        executionService = mock(ExecutionService.class);
        workflowExecutor = mock(WorkflowExecutor.class);
        actionProcessor = mock(SimpleActionProcessor.class);
        parametersUtils = new ParametersUtils(objectMapper);
        jsonUtils = new JsonUtils(objectMapper);

        queue = mock(ObservableQueue.class);
        message =
                new Message(
                        "t0",
                        "{\"Type\":\"Notification\",\"MessageId\":\"7e4e6415-01e9-5caf-abaa-37fd05d446ff\",\"Message\":\"{\\n    \\\"testKey1\\\": \\\"level1\\\",\\n    \\\"metadata\\\": {\\n      \\\"testKey2\\\": 123456 }\\n  }\",\"Timestamp\":\"2018-08-10T21:22:05.029Z\",\"SignatureVersion\":\"1\"}",
                        "t0");

        when(queue.getURI()).thenReturn(queueURI);
        when(queue.getName()).thenReturn(queueURI);
        when(queue.getType()).thenReturn("sqs");

        properties = mock(ConductorProperties.class);
        when(properties.isEventMessageIndexingEnabled()).thenReturn(true);
        when(properties.getEventProcessorThreadCount()).thenReturn(2);
    }

    @Test
    public void testEventProcessor() {
        // setup event handler
        EventHandler eventHandler = new EventHandler();
        eventHandler.setName(UUID.randomUUID().toString());
        eventHandler.setActive(true);

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "dev");

        Action startWorkflowAction = new Action();
        startWorkflowAction.setAction(Type.start_workflow);
        startWorkflowAction.setStart_workflow(new StartWorkflow());
        startWorkflowAction.getStart_workflow().setName("workflow_x");
        startWorkflowAction.getStart_workflow().setVersion(1);
        startWorkflowAction.getStart_workflow().setTaskToDomain(taskToDomain);
        eventHandler.getActions().add(startWorkflowAction);

        Action completeTaskAction = new Action();
        completeTaskAction.setAction(Type.complete_task);
        completeTaskAction.setComplete_task(new TaskDetails());
        completeTaskAction.getComplete_task().setTaskRefName("task_x");
        completeTaskAction.getComplete_task().setWorkflowId(UUID.randomUUID().toString());
        completeTaskAction.getComplete_task().setOutput(new HashMap<>());
        eventHandler.getActions().add(completeTaskAction);

        eventHandler.setEvent(event);

        when(metadataService.getEventHandlersForEvent(event, true))
                .thenReturn(Collections.singletonList(eventHandler));
        when(executionService.addEventExecution(any())).thenReturn(true);
        when(queue.rePublishIfNoAck()).thenReturn(false);

        String id = UUID.randomUUID().toString();
        AtomicBoolean started = new AtomicBoolean(false);
        doAnswer(
                        (Answer<String>)
                                invocation -> {
                                    started.set(true);
                                    return id;
                                })
                .when(workflowExecutor)
                .startWorkflow(
                        eq(startWorkflowAction.getStart_workflow().getName()),
                        eq(startWorkflowAction.getStart_workflow().getVersion()),
                        eq(startWorkflowAction.getStart_workflow().getCorrelationId()),
                        anyMap(),
                        eq(null),
                        eq(event),
                        anyMap());

        AtomicBoolean completed = new AtomicBoolean(false);
        doAnswer(
                        (Answer<String>)
                                invocation -> {
                                    completed.set(true);
                                    return null;
                                })
                .when(workflowExecutor)
                .updateTask(any());

        Task task = new Task();
        task.setReferenceTaskName(completeTaskAction.getComplete_task().getTaskRefName());
        Workflow workflow = new Workflow();
        workflow.setTasks(Collections.singletonList(task));
        when(workflowExecutor.getWorkflow(
                        completeTaskAction.getComplete_task().getWorkflowId(), true))
                .thenReturn(workflow);

        SimpleActionProcessor actionProcessor =
                new SimpleActionProcessor(workflowExecutor, parametersUtils, jsonUtils);

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        eventProcessor.handle(queue, message);
        assertTrue(started.get());
        assertTrue(completed.get());
        verify(queue, atMost(1)).ack(any());
        verify(queue, never()).publish(any());
    }

    @Test
    public void testEventHandlerWithCondition() {
        EventHandler eventHandler = new EventHandler();
        eventHandler.setName("cms_intermediate_video_ingest_handler");
        eventHandler.setActive(true);
        eventHandler.setEvent("sqs:dev_cms_asset_ingest_queue");
        eventHandler.setCondition(
                "$.Message.testKey1 == 'level1' && $.Message.metadata.testKey2 == 123456");

        Map<String, Object> startWorkflowInput = new LinkedHashMap<>();
        startWorkflowInput.put("param1", "${Message.metadata.testKey2}");
        startWorkflowInput.put("param2", "SQS-${MessageId}");

        Action startWorkflowAction = new Action();
        startWorkflowAction.setAction(Type.start_workflow);
        startWorkflowAction.setStart_workflow(new StartWorkflow());
        startWorkflowAction.getStart_workflow().setName("cms_artwork_automation");
        startWorkflowAction.getStart_workflow().setVersion(1);
        startWorkflowAction.getStart_workflow().setInput(startWorkflowInput);
        startWorkflowAction.setExpandInlineJSON(true);
        eventHandler.getActions().add(startWorkflowAction);

        eventHandler.setEvent(event);

        when(metadataService.getEventHandlersForEvent(event, true))
                .thenReturn(Collections.singletonList(eventHandler));
        when(executionService.addEventExecution(any())).thenReturn(true);
        when(queue.rePublishIfNoAck()).thenReturn(false);

        String id = UUID.randomUUID().toString();
        AtomicBoolean started = new AtomicBoolean(false);
        doAnswer(
                        (Answer<String>)
                                invocation -> {
                                    started.set(true);
                                    return id;
                                })
                .when(workflowExecutor)
                .startWorkflow(
                        eq(startWorkflowAction.getStart_workflow().getName()),
                        eq(startWorkflowAction.getStart_workflow().getVersion()),
                        eq(startWorkflowAction.getStart_workflow().getCorrelationId()),
                        anyMap(),
                        eq(null),
                        eq(event),
                        eq(null));

        SimpleActionProcessor actionProcessor =
                new SimpleActionProcessor(workflowExecutor, parametersUtils, jsonUtils);

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        eventProcessor.handle(queue, message);
        assertTrue(started.get());
    }

    @Test
    public void testEventHandlerWithConditionEvaluator() {
        EventHandler eventHandler = new EventHandler();
        eventHandler.setName("cms_intermediate_video_ingest_handler");
        eventHandler.setActive(true);
        eventHandler.setEvent("sqs:dev_cms_asset_ingest_queue");
        eventHandler.setEvaluatorType(JavascriptEvaluator.NAME);
        eventHandler.setCondition(
                "$.Message.testKey1 == 'level1' && $.Message.metadata.testKey2 == 123456");

        Map<String, Object> startWorkflowInput = new LinkedHashMap<>();
        startWorkflowInput.put("param1", "${Message.metadata.testKey2}");
        startWorkflowInput.put("param2", "SQS-${MessageId}");

        Action startWorkflowAction = new Action();
        startWorkflowAction.setAction(Type.start_workflow);
        startWorkflowAction.setStart_workflow(new StartWorkflow());
        startWorkflowAction.getStart_workflow().setName("cms_artwork_automation");
        startWorkflowAction.getStart_workflow().setVersion(1);
        startWorkflowAction.getStart_workflow().setInput(startWorkflowInput);
        startWorkflowAction.setExpandInlineJSON(true);
        eventHandler.getActions().add(startWorkflowAction);

        eventHandler.setEvent(event);

        when(metadataService.getEventHandlersForEvent(event, true))
                .thenReturn(Collections.singletonList(eventHandler));
        when(executionService.addEventExecution(any())).thenReturn(true);
        when(queue.rePublishIfNoAck()).thenReturn(false);

        String id = UUID.randomUUID().toString();
        AtomicBoolean started = new AtomicBoolean(false);
        doAnswer(
                        (Answer<String>)
                                invocation -> {
                                    started.set(true);
                                    return id;
                                })
                .when(workflowExecutor)
                .startWorkflow(
                        eq(startWorkflowAction.getStart_workflow().getName()),
                        eq(startWorkflowAction.getStart_workflow().getVersion()),
                        eq(startWorkflowAction.getStart_workflow().getCorrelationId()),
                        anyMap(),
                        eq(null),
                        eq(event),
                        eq(null));

        SimpleActionProcessor actionProcessor =
                new SimpleActionProcessor(workflowExecutor, parametersUtils, jsonUtils);

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        eventProcessor.handle(queue, message);
        assertTrue(started.get());
    }

    @Test
    public void testEventProcessorWithRetriableError() {
        EventHandler eventHandler = new EventHandler();
        eventHandler.setName(UUID.randomUUID().toString());
        eventHandler.setActive(true);
        eventHandler.setEvent(event);

        Action completeTaskAction = new Action();
        completeTaskAction.setAction(Type.complete_task);
        completeTaskAction.setComplete_task(new TaskDetails());
        completeTaskAction.getComplete_task().setTaskRefName("task_x");
        completeTaskAction.getComplete_task().setWorkflowId(UUID.randomUUID().toString());
        completeTaskAction.getComplete_task().setOutput(new HashMap<>());
        eventHandler.getActions().add(completeTaskAction);

        when(queue.rePublishIfNoAck()).thenReturn(false);
        when(metadataService.getEventHandlersForEvent(event, true))
                .thenReturn(Collections.singletonList(eventHandler));
        when(executionService.addEventExecution(any())).thenReturn(true);
        when(actionProcessor.execute(any(), any(), any(), any()))
                .thenThrow(
                        new ApplicationException(
                                ApplicationException.Code.BACKEND_ERROR, "some retriable error"));

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        eventProcessor.handle(queue, message);
        verify(queue, never()).ack(any());
        verify(queue, never()).publish(any());
    }

    @Test
    public void testEventProcessorWithNonRetriableError() {
        EventHandler eventHandler = new EventHandler();
        eventHandler.setName(UUID.randomUUID().toString());
        eventHandler.setActive(true);
        eventHandler.setEvent(event);

        Action completeTaskAction = new Action();
        completeTaskAction.setAction(Type.complete_task);
        completeTaskAction.setComplete_task(new TaskDetails());
        completeTaskAction.getComplete_task().setTaskRefName("task_x");
        completeTaskAction.getComplete_task().setWorkflowId(UUID.randomUUID().toString());
        completeTaskAction.getComplete_task().setOutput(new HashMap<>());
        eventHandler.getActions().add(completeTaskAction);

        when(metadataService.getEventHandlersForEvent(event, true))
                .thenReturn(Collections.singletonList(eventHandler));
        when(executionService.addEventExecution(any())).thenReturn(true);

        when(actionProcessor.execute(any(), any(), any(), any()))
                .thenThrow(
                        new ApplicationException(
                                ApplicationException.Code.INVALID_INPUT,
                                "some non-retriable error"));

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        eventProcessor.handle(queue, message);
        verify(queue, atMost(1)).ack(any());
        verify(queue, never()).publish(any());
    }

    @Test
    public void testExecuteInvalidAction() {
        AtomicInteger executeInvoked = new AtomicInteger(0);
        doAnswer(
                        (Answer<Map<String, Object>>)
                                invocation -> {
                                    executeInvoked.incrementAndGet();
                                    throw new UnsupportedOperationException("error");
                                })
                .when(actionProcessor)
                .execute(any(), any(), any(), any());

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        EventExecution eventExecution = new EventExecution("id", "messageId");
        eventExecution.setName("handler");
        eventExecution.setStatus(EventExecution.Status.IN_PROGRESS);
        eventExecution.setEvent("event");
        Action action = new Action();
        eventExecution.setAction(Type.start_workflow);

        eventProcessor.execute(eventExecution, action, "payload");
        assertEquals(1, executeInvoked.get());
        assertEquals(EventExecution.Status.FAILED, eventExecution.getStatus());
        assertNotNull(eventExecution.getOutput().get("exception"));
    }

    @Test
    public void testExecuteNonRetriableApplicationException() {
        AtomicInteger executeInvoked = new AtomicInteger(0);
        doAnswer(
                        (Answer<Map<String, Object>>)
                                invocation -> {
                                    executeInvoked.incrementAndGet();
                                    throw new ApplicationException(
                                            ApplicationException.Code.INVALID_INPUT,
                                            "some non-retriable error");
                                })
                .when(actionProcessor)
                .execute(any(), any(), any(), any());

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        EventExecution eventExecution = new EventExecution("id", "messageId");
        eventExecution.setStatus(EventExecution.Status.IN_PROGRESS);
        eventExecution.setEvent("event");
        eventExecution.setName("handler");
        Action action = new Action();
        action.setAction(Type.start_workflow);
        eventExecution.setAction(Type.start_workflow);

        eventProcessor.execute(eventExecution, action, "payload");
        assertEquals(1, executeInvoked.get());
        assertEquals(EventExecution.Status.FAILED, eventExecution.getStatus());
        assertNotNull(eventExecution.getOutput().get("exception"));
    }

    @Test
    public void testExecuteRetriableApplicationException() {
        AtomicInteger executeInvoked = new AtomicInteger(0);
        doAnswer(
                        (Answer<Map<String, Object>>)
                                invocation -> {
                                    executeInvoked.incrementAndGet();
                                    throw new ApplicationException(
                                            ApplicationException.Code.BACKEND_ERROR,
                                            "some retriable error");
                                })
                .when(actionProcessor)
                .execute(any(), any(), any(), any());

        DefaultEventProcessor eventProcessor =
                new DefaultEventProcessor(
                        executionService,
                        metadataService,
                        actionProcessor,
                        jsonUtils,
                        properties,
                        objectMapper,
                        evaluators);
        EventExecution eventExecution = new EventExecution("id", "messageId");
        eventExecution.setStatus(EventExecution.Status.IN_PROGRESS);
        eventExecution.setEvent("event");
        Action action = new Action();
        action.setAction(Type.start_workflow);

        eventProcessor.execute(eventExecution, action, "payload");
        assertEquals(3, executeInvoked.get());
        assertNull(eventExecution.getOutput().get("exception"));
    }
}

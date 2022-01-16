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
package com.netflix.conductor.core.execution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.exception.ApplicationException;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.execution.evaluators.Evaluator;
import com.netflix.conductor.core.execution.mapper.DecisionTaskMapper;
import com.netflix.conductor.core.execution.mapper.DynamicTaskMapper;
import com.netflix.conductor.core.execution.mapper.EventTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinDynamicTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.HTTPTaskMapper;
import com.netflix.conductor.core.execution.mapper.InlineTaskMapper;
import com.netflix.conductor.core.execution.mapper.JoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.LambdaTaskMapper;
import com.netflix.conductor.core.execution.mapper.SimpleTaskMapper;
import com.netflix.conductor.core.execution.mapper.SubWorkflowTaskMapper;
import com.netflix.conductor.core.execution.mapper.SwitchTaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.UserDefinedTaskMapper;
import com.netflix.conductor.core.execution.mapper.WaitTaskMapper;
import com.netflix.conductor.core.execution.tasks.Lambda;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionLockService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;

import static com.netflix.conductor.common.metadata.tasks.TaskType.DECISION;
import static com.netflix.conductor.common.metadata.tasks.TaskType.DYNAMIC;
import static com.netflix.conductor.common.metadata.tasks.TaskType.EVENT;
import static com.netflix.conductor.common.metadata.tasks.TaskType.FORK_JOIN;
import static com.netflix.conductor.common.metadata.tasks.TaskType.FORK_JOIN_DYNAMIC;
import static com.netflix.conductor.common.metadata.tasks.TaskType.HTTP;
import static com.netflix.conductor.common.metadata.tasks.TaskType.INLINE;
import static com.netflix.conductor.common.metadata.tasks.TaskType.JOIN;
import static com.netflix.conductor.common.metadata.tasks.TaskType.LAMBDA;
import static com.netflix.conductor.common.metadata.tasks.TaskType.SIMPLE;
import static com.netflix.conductor.common.metadata.tasks.TaskType.SUB_WORKFLOW;
import static com.netflix.conductor.common.metadata.tasks.TaskType.SWITCH;
import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_JSON_JQ_TRANSFORM;
import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_LAMBDA;
import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SUB_WORKFLOW;
import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;
import static com.netflix.conductor.common.metadata.tasks.TaskType.USER_DEFINED;
import static com.netflix.conductor.common.metadata.tasks.TaskType.WAIT;
import static com.netflix.conductor.common.run.Workflow.WorkflowStatus.COMPLETED;
import static com.netflix.conductor.common.run.Workflow.WorkflowStatus.PAUSED;
import static com.netflix.conductor.common.run.Workflow.WorkflowStatus.RUNNING;
import static com.netflix.conductor.core.exception.ApplicationException.Code.CONFLICT;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.maxBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ContextConfiguration(
        classes = {
            TestObjectMapperConfiguration.class,
            TestWorkflowExecutor.TestConfiguration.class
        })
@RunWith(SpringRunner.class)
public class TestWorkflowExecutor {

    private WorkflowExecutor workflowExecutor;
    private ExecutionDAOFacade executionDAOFacade;
    private MetadataDAO metadataDAO;
    private QueueDAO queueDAO;
    private WorkflowStatusListener workflowStatusListener;
    private ExecutionLockService executionLockService;
    private ExternalPayloadStorageUtils externalPayloadStorageUtils;

    @Configuration
    @ComponentScan(basePackageClasses = {Evaluator.class}) // load all Evaluator beans.
    public static class TestConfiguration {

        @Bean(TASK_TYPE_SUB_WORKFLOW)
        public SubWorkflow subWorkflow(ObjectMapper objectMapper) {
            return new SubWorkflow(objectMapper);
        }

        @Bean(TASK_TYPE_LAMBDA)
        public Lambda lambda() {
            return new Lambda();
        }

        @Bean(TASK_TYPE_WAIT)
        public Wait waitBean() {
            return new Wait();
        }

        @Bean("HTTP")
        public WorkflowSystemTask http() {
            return new WorkflowSystemTaskStub("HTTP") {
                @Override
                public boolean isAsync() {
                    return true;
                }
            };
        }

        @Bean("HTTP2")
        public WorkflowSystemTask http2() {
            return new WorkflowSystemTaskStub("HTTP2");
        }

        @Bean(TASK_TYPE_JSON_JQ_TRANSFORM)
        public WorkflowSystemTask jsonBean() {
            return new WorkflowSystemTaskStub("JSON_JQ_TRANSFORM") {
                @Override
                public boolean isAsync() {
                    return false;
                }

                @Override
                public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
                    task.setStatus(Task.Status.COMPLETED);
                }
            };
        }

        @Bean
        public SystemTaskRegistry systemTaskRegistry(Set<WorkflowSystemTask> tasks) {
            return new SystemTaskRegistry(tasks);
        }
    }

    @Autowired private ObjectMapper objectMapper;

    @Autowired private SystemTaskRegistry systemTaskRegistry;

    @Autowired private DefaultListableBeanFactory beanFactory;

    @Autowired private Map<String, Evaluator> evaluators;

    @Before
    public void init() {
        executionDAOFacade = mock(ExecutionDAOFacade.class);
        metadataDAO = mock(MetadataDAO.class);
        queueDAO = mock(QueueDAO.class);
        workflowStatusListener = mock(WorkflowStatusListener.class);
        externalPayloadStorageUtils = mock(ExternalPayloadStorageUtils.class);
        executionLockService = mock(ExecutionLockService.class);
        ParametersUtils parametersUtils = new ParametersUtils(objectMapper);
        Map<TaskType, TaskMapper> taskMappers = new HashMap<>();
        taskMappers.put(DECISION, new DecisionTaskMapper());
        taskMappers.put(SWITCH, new SwitchTaskMapper(evaluators));
        taskMappers.put(DYNAMIC, new DynamicTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put(FORK_JOIN, new ForkJoinTaskMapper());
        taskMappers.put(JOIN, new JoinTaskMapper());
        taskMappers.put(
                FORK_JOIN_DYNAMIC,
                new ForkJoinDynamicTaskMapper(parametersUtils, objectMapper, metadataDAO));
        taskMappers.put(USER_DEFINED, new UserDefinedTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put(SIMPLE, new SimpleTaskMapper(parametersUtils));
        taskMappers.put(SUB_WORKFLOW, new SubWorkflowTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put(EVENT, new EventTaskMapper(parametersUtils));
        taskMappers.put(WAIT, new WaitTaskMapper(parametersUtils));
        taskMappers.put(HTTP, new HTTPTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put(LAMBDA, new LambdaTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put(INLINE, new InlineTaskMapper(parametersUtils, metadataDAO));

        DeciderService deciderService =
                new DeciderService(
                        parametersUtils,
                        metadataDAO,
                        externalPayloadStorageUtils,
                        systemTaskRegistry,
                        taskMappers,
                        Duration.ofMinutes(60));
        MetadataMapperService metadataMapperService = new MetadataMapperService(metadataDAO);

        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getActiveWorkerLastPollTimeout()).thenReturn(Duration.ofSeconds(100));
        when(properties.getTaskExecutionPostponeDuration()).thenReturn(Duration.ofSeconds(60));
        when(properties.getWorkflowOffsetTimeout()).thenReturn(Duration.ofSeconds(30));

        workflowExecutor =
                new WorkflowExecutor(
                        deciderService,
                        metadataDAO,
                        queueDAO,
                        metadataMapperService,
                        workflowStatusListener,
                        executionDAOFacade,
                        properties,
                        executionLockService,
                        systemTaskRegistry,
                        parametersUtils);
    }

    @Test
    public void testScheduleTask() {
        WorkflowSystemTaskStub httpTask = beanFactory.getBean("HTTP", WorkflowSystemTaskStub.class);
        WorkflowSystemTaskStub http2Task =
                beanFactory.getBean("HTTP2", WorkflowSystemTaskStub.class);

        Workflow workflow = new Workflow();
        workflow.setWorkflowId("1");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("1");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        List<Task> tasks = new LinkedList<>();

        WorkflowTask taskToSchedule = new WorkflowTask();
        taskToSchedule.setWorkflowTaskType(TaskType.USER_DEFINED);
        taskToSchedule.setType("HTTP");

        WorkflowTask taskToSchedule2 = new WorkflowTask();
        taskToSchedule2.setWorkflowTaskType(TaskType.USER_DEFINED);
        taskToSchedule2.setType("HTTP2");

        WorkflowTask wait = new WorkflowTask();
        wait.setWorkflowTaskType(TaskType.WAIT);
        wait.setType("WAIT");
        wait.setTaskReferenceName("wait");

        Task task1 = new Task();
        task1.setTaskType(taskToSchedule.getType());
        task1.setTaskDefName(taskToSchedule.getName());
        task1.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        task1.setWorkflowInstanceId(workflow.getWorkflowId());
        task1.setCorrelationId(workflow.getCorrelationId());
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(IDGenerator.generate());
        task1.setInputData(new HashMap<>());
        task1.setStatus(Status.SCHEDULED);
        task1.setRetryCount(0);
        task1.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
        task1.setWorkflowTask(taskToSchedule);

        Task task2 = new Task();
        task2.setTaskType(TASK_TYPE_WAIT);
        task2.setTaskDefName(taskToSchedule.getName());
        task2.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        task2.setWorkflowInstanceId(workflow.getWorkflowId());
        task2.setCorrelationId(workflow.getCorrelationId());
        task2.setScheduledTime(System.currentTimeMillis());
        task2.setInputData(new HashMap<>());
        task2.setTaskId(IDGenerator.generate());
        task2.setStatus(Status.IN_PROGRESS);
        task2.setWorkflowTask(taskToSchedule);

        Task task3 = new Task();
        task3.setTaskType(taskToSchedule2.getType());
        task3.setTaskDefName(taskToSchedule.getName());
        task3.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        task3.setWorkflowInstanceId(workflow.getWorkflowId());
        task3.setCorrelationId(workflow.getCorrelationId());
        task3.setScheduledTime(System.currentTimeMillis());
        task3.setTaskId(IDGenerator.generate());
        task3.setInputData(new HashMap<>());
        task3.setStatus(Status.SCHEDULED);
        task3.setRetryCount(0);
        task3.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
        task3.setWorkflowTask(taskToSchedule);

        tasks.add(task1);
        tasks.add(task2);
        tasks.add(task3);

        when(executionDAOFacade.createTasks(tasks)).thenReturn(tasks);
        AtomicInteger startedTaskCount = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            startedTaskCount.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTask(any());

        AtomicInteger queuedTaskCount = new AtomicInteger(0);
        final Answer answer =
                invocation -> {
                    String queueName = invocation.getArgument(0, String.class);
                    queuedTaskCount.incrementAndGet();
                    return null;
                };
        doAnswer(answer).when(queueDAO).push(any(), any(), anyLong());
        doAnswer(answer).when(queueDAO).push(any(), any(), anyInt(), anyLong());

        boolean stateChanged = workflowExecutor.scheduleTask(workflow, tasks);
        assertEquals(2, startedTaskCount.get());
        assertEquals(1, queuedTaskCount.get());
        assertTrue(stateChanged);
        assertFalse(httpTask.isStarted());
        assertTrue(http2Task.isStarted());
    }

    @Test(expected = TerminateWorkflowException.class)
    public void testScheduleTaskFailure() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("wid_01");

        List<Task> tasks = new LinkedList<>();

        Task task1 = new Task();
        task1.setTaskType(TaskType.TASK_TYPE_SIMPLE);
        task1.setTaskDefName("task_1");
        task1.setReferenceTaskName("task_1");
        task1.setWorkflowInstanceId(workflow.getWorkflowId());
        task1.setTaskId("tid_01");
        task1.setStatus(Status.SCHEDULED);
        task1.setRetryCount(0);

        tasks.add(task1);

        when(executionDAOFacade.createTasks(tasks)).thenThrow(new RuntimeException());
        workflowExecutor.scheduleTask(workflow, tasks);
    }

    /** Simulate Queue push failures and assert that scheduleTask doesn't throw an exception. */
    @Test
    public void testQueueFailuresDuringScheduleTask() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("wid_01");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("wid");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        List<Task> tasks = new LinkedList<>();

        Task task1 = new Task();
        task1.setTaskType(TaskType.TASK_TYPE_SIMPLE);
        task1.setTaskDefName("task_1");
        task1.setReferenceTaskName("task_1");
        task1.setWorkflowInstanceId(workflow.getWorkflowId());
        task1.setTaskId("tid_01");
        task1.setStatus(Status.SCHEDULED);
        task1.setRetryCount(0);

        tasks.add(task1);

        when(executionDAOFacade.createTasks(tasks)).thenReturn(tasks);
        doThrow(new RuntimeException())
                .when(queueDAO)
                .push(anyString(), anyString(), anyInt(), anyLong());
        assertFalse(workflowExecutor.scheduleTask(workflow, tasks));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompleteWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("test");

        Workflow workflow = new Workflow();
        workflow.setWorkflowDefinition(def);
        workflow.setWorkflowId("1");
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflow.setOwnerApp("junit_test");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        workflow.setOutput(Collections.EMPTY_MAP);

        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateWorkflowCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateTasksCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTasks(any());

        AtomicInteger removeQueueEntryCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            removeQueueEntryCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(queueDAO)
                .remove(anyString(), anyString());

        workflowExecutor.completeWorkflow(workflow);
        assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
        assertEquals(1, updateWorkflowCalledCounter.get());
        assertEquals(0, updateTasksCalledCounter.get());
        assertEquals(0, removeQueueEntryCalledCounter.get());
        verify(workflowStatusListener, times(1)).onWorkflowCompletedIfEnabled(any(Workflow.class));
        verify(workflowStatusListener, times(0)).onWorkflowFinalizedIfEnabled(any(Workflow.class));

        def.setWorkflowStatusListenerEnabled(true);
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflowExecutor.completeWorkflow(workflow);
        verify(workflowStatusListener, times(2)).onWorkflowCompletedIfEnabled(any(Workflow.class));
        verify(workflowStatusListener, times(0)).onWorkflowFinalizedIfEnabled(any(Workflow.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTerminateWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("test");

        Workflow workflow = new Workflow();
        workflow.setWorkflowDefinition(def);
        workflow.setWorkflowId("1");
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflow.setOwnerApp("junit_test");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        workflow.setOutput(Collections.EMPTY_MAP);

        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateWorkflowCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateTasksCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTasks(any());

        AtomicInteger removeQueueEntryCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            removeQueueEntryCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(queueDAO)
                .remove(anyString(), anyString());

        workflowExecutor.terminateWorkflow("workflowId", "reason");
        assertEquals(Workflow.WorkflowStatus.TERMINATED, workflow.getStatus());
        assertEquals(1, updateWorkflowCalledCounter.get());
        assertEquals(1, removeQueueEntryCalledCounter.get());

        verify(workflowStatusListener, times(1)).onWorkflowTerminatedIfEnabled(any(Workflow.class));
        verify(workflowStatusListener, times(1)).onWorkflowFinalizedIfEnabled(any(Workflow.class));

        def.setWorkflowStatusListenerEnabled(true);
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflowExecutor.completeWorkflow(workflow);
        verify(workflowStatusListener, times(1)).onWorkflowCompletedIfEnabled(any(Workflow.class));
        verify(workflowStatusListener, times(1)).onWorkflowFinalizedIfEnabled(any(Workflow.class));
    }

    @Test
    public void testUploadOutputFailuresDuringTerminateWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("test");
        def.setWorkflowStatusListenerEnabled(true);

        Workflow workflow = new Workflow();
        workflow.setWorkflowDefinition(def);
        workflow.setWorkflowId("1");
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflow.setOwnerApp("junit_test");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        workflow.setOutput(Collections.EMPTY_MAP);

        List<Task> tasks = new LinkedList<>();

        Task task = new Task();
        task.setScheduledTime(1L);
        task.setSeq(1);
        task.setTaskId(UUID.randomUUID().toString());
        task.setReferenceTaskName("t1");
        task.setWorkflowInstanceId(workflow.getWorkflowId());
        task.setTaskDefName("task1");
        task.setStatus(Status.IN_PROGRESS);

        tasks.add(task);
        workflow.setTasks(tasks);

        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateWorkflowCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateWorkflow(any());

        doThrow(new RuntimeException("any exception"))
                .when(externalPayloadStorageUtils)
                .verifyAndUpload(workflow, ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT);

        workflowExecutor.terminateWorkflow(workflow.getWorkflowId(), "reason");
        assertEquals(Workflow.WorkflowStatus.TERMINATED, workflow.getStatus());
        assertEquals(1, updateWorkflowCalledCounter.get());
        verify(workflowStatusListener, times(1)).onWorkflowTerminatedIfEnabled(any(Workflow.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueueExceptionsIgnoredDuringTerminateWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("test");
        def.setWorkflowStatusListenerEnabled(true);

        Workflow workflow = new Workflow();
        workflow.setWorkflowDefinition(def);
        workflow.setWorkflowId("1");
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflow.setOwnerApp("junit_test");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        workflow.setOutput(Collections.EMPTY_MAP);

        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateWorkflowCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateTasksCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTasks(any());

        doThrow(new RuntimeException()).when(queueDAO).remove(anyString(), anyString());

        workflowExecutor.terminateWorkflow("workflowId", "reason");
        assertEquals(Workflow.WorkflowStatus.TERMINATED, workflow.getStatus());
        assertEquals(1, updateWorkflowCalledCounter.get());
        verify(workflowStatusListener, times(1)).onWorkflowTerminatedIfEnabled(any(Workflow.class));
    }

    @Test
    public void testRestartWorkflow() {
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("test_task");
        workflowTask.setTaskReferenceName("task_ref");

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testDef");
        workflowDef.setVersion(1);
        workflowDef.setRestartable(true);
        workflowDef.getTasks().addAll(Collections.singletonList(workflowTask));

        Task task_1 = new Task();
        task_1.setTaskId(UUID.randomUUID().toString());
        task_1.setSeq(1);
        task_1.setStatus(Status.FAILED);
        task_1.setTaskDefName(workflowTask.getName());
        task_1.setReferenceTaskName(workflowTask.getTaskReferenceName());

        Task task_2 = new Task();
        task_2.setTaskId(UUID.randomUUID().toString());
        task_2.setSeq(2);
        task_2.setStatus(Status.FAILED);
        task_2.setTaskDefName(workflowTask.getName());
        task_2.setReferenceTaskName(workflowTask.getTaskReferenceName());

        Workflow workflow = new Workflow();
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setWorkflowId("test-workflow-id");
        workflow.getTasks().addAll(Arrays.asList(task_1, task_2));
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);
        workflow.setEndTime(500);
        workflow.setLastRetriedTime(100);

        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        doNothing().when(executionDAOFacade).removeTask(any());
        when(metadataDAO.getWorkflowDef(workflow.getWorkflowName(), workflow.getWorkflowVersion()))
                .thenReturn(Optional.of(workflowDef));
        when(metadataDAO.getTaskDef(workflowTask.getName())).thenReturn(new TaskDef());
        when(executionDAOFacade.updateWorkflow(any())).thenReturn("");

        workflowExecutor.restart(workflow.getWorkflowId(), false);
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertEquals(0, workflow.getEndTime());
        assertEquals(0, workflow.getLastRetriedTime());
        verify(metadataDAO, never()).getLatestWorkflowDef(any());

        ArgumentCaptor<Workflow> argumentCaptor = ArgumentCaptor.forClass(Workflow.class);
        verify(executionDAOFacade, times(1)).createWorkflow(argumentCaptor.capture());
        assertEquals(
                workflow.getWorkflowId(), argumentCaptor.getAllValues().get(0).getWorkflowId());
        assertEquals(
                workflow.getWorkflowDefinition(),
                argumentCaptor.getAllValues().get(0).getWorkflowDefinition());

        // add a new version of the workflow definition and restart with latest
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        workflow.setEndTime(500);
        workflow.setLastRetriedTime(100);
        workflowDef = new WorkflowDef();
        workflowDef.setName("testDef");
        workflowDef.setVersion(2);
        workflowDef.setRestartable(true);
        workflowDef.getTasks().addAll(Collections.singletonList(workflowTask));

        when(metadataDAO.getLatestWorkflowDef(workflow.getWorkflowName()))
                .thenReturn(Optional.of(workflowDef));
        workflowExecutor.restart(workflow.getWorkflowId(), true);
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertEquals(0, workflow.getEndTime());
        assertEquals(0, workflow.getLastRetriedTime());
        verify(metadataDAO, times(1)).getLatestWorkflowDef(anyString());

        argumentCaptor = ArgumentCaptor.forClass(Workflow.class);
        verify(executionDAOFacade, times(2)).createWorkflow(argumentCaptor.capture());
        assertEquals(
                workflow.getWorkflowId(), argumentCaptor.getAllValues().get(1).getWorkflowId());
        assertEquals(workflowDef, argumentCaptor.getAllValues().get(1).getWorkflowDefinition());
    }

    @Test(expected = ApplicationException.class)
    public void testRetryNonTerminalWorkflow() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryNonTerminalWorkflow");
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.retry(workflow.getWorkflowId(), false);
    }

    @Test(expected = ApplicationException.class)
    public void testRetryWorkflowNoTasks() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("ApplicationException");
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);
        workflow.setTasks(Collections.emptyList());
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.retry(workflow.getWorkflowId(), false);
    }

    @Test(expected = ApplicationException.class)
    public void testRetryWorkflowNoFailedTasks() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        // add 2 failed task in 2 forks and 1 cancelled in the 3rd fork
        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(1);
        task_1_1.setRetryCount(0);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setTaskDefName("task1");
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_1_2 = new Task();
        task_1_2.setTaskId(UUID.randomUUID().toString());
        task_1_2.setSeq(2);
        task_1_2.setRetryCount(1);
        task_1_2.setTaskType(TaskType.SIMPLE.toString());
        task_1_2.setStatus(Status.COMPLETED);
        task_1_2.setTaskDefName("task1");
        task_1_2.setReferenceTaskName("task1_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_1_2));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));

        workflowExecutor.retry(workflow.getWorkflowId(), false);
    }

    @Test
    public void testRetryWorkflow() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateWorkflowCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateTasksCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTasks(any());

        AtomicInteger updateTaskCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateTaskCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTask(any());

        // add 2 failed task in 2 forks and 1 cancelled in the 3rd fork
        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(1);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.CANCELED);
        task_1_1.setRetried(true);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_1_2 = new Task();
        task_1_2.setTaskId(UUID.randomUUID().toString());
        task_1_2.setSeq(21);
        task_1_2.setRetryCount(1);
        task_1_2.setTaskType(TaskType.SIMPLE.toString());
        task_1_2.setStatus(Status.FAILED);
        task_1_2.setTaskDefName("task1");
        task_1_2.setWorkflowTask(new WorkflowTask());
        task_1_2.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.FAILED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        Task task_3_1 = new Task();
        task_3_1.setTaskId(UUID.randomUUID().toString());
        task_3_1.setSeq(23);
        task_3_1.setRetryCount(1);
        task_3_1.setStatus(Status.CANCELED);
        task_3_1.setTaskType(TaskType.SIMPLE.toString());
        task_3_1.setTaskDefName("task3");
        task_3_1.setWorkflowTask(new WorkflowTask());
        task_3_1.setReferenceTaskName("task3_ref1");

        Task task_4_1 = new Task();
        task_4_1.setTaskId(UUID.randomUUID().toString());
        task_4_1.setSeq(122);
        task_4_1.setRetryCount(1);
        task_4_1.setStatus(Status.FAILED);
        task_4_1.setTaskType(TaskType.SIMPLE.toString());
        task_4_1.setTaskDefName("task1");
        task_4_1.setWorkflowTask(new WorkflowTask());
        task_4_1.setReferenceTaskName("task4_refABC");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_1_2, task_2_1, task_3_1, task_4_1));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        // then:
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertEquals(1, updateWorkflowCalledCounter.get());
        assertEquals(1, updateTasksCalledCounter.get());
        assertEquals(0, updateTaskCalledCounter.get());
    }

    @Test
    public void testRetryWorkflowReturnsNoDuplicates() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(10);
        task_1_1.setRetryCount(0);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_1_2 = new Task();
        task_1_2.setTaskId(UUID.randomUUID().toString());
        task_1_2.setSeq(11);
        task_1_2.setRetryCount(1);
        task_1_2.setTaskType(TaskType.SIMPLE.toString());
        task_1_2.setStatus(Status.COMPLETED);
        task_1_2.setTaskDefName("task1");
        task_1_2.setWorkflowTask(new WorkflowTask());
        task_1_2.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(21);
        task_2_1.setRetryCount(0);
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        Task task_3_1 = new Task();
        task_3_1.setTaskId(UUID.randomUUID().toString());
        task_3_1.setSeq(31);
        task_3_1.setRetryCount(1);
        task_3_1.setStatus(Status.FAILED_WITH_TERMINAL_ERROR);
        task_3_1.setTaskType(TaskType.SIMPLE.toString());
        task_3_1.setTaskDefName("task1");
        task_3_1.setWorkflowTask(new WorkflowTask());
        task_3_1.setReferenceTaskName("task3_ref1");

        Task task_4_1 = new Task();
        task_4_1.setTaskId(UUID.randomUUID().toString());
        task_4_1.setSeq(41);
        task_4_1.setRetryCount(0);
        task_4_1.setStatus(Status.TIMED_OUT);
        task_4_1.setTaskType(TaskType.SIMPLE.toString());
        task_4_1.setTaskDefName("task1");
        task_4_1.setWorkflowTask(new WorkflowTask());
        task_4_1.setReferenceTaskName("task4_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_1_2, task_2_1, task_3_1, task_4_1));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        assertEquals(8, workflow.getTasks().size());
    }

    @Test
    public void testRetryWorkflowMultipleRetries() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(10);
        task_1_1.setRetryCount(0);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(20);
        task_2_1.setRetryCount(0);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskDefName("task1");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_2_1));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        assertEquals(4, workflow.getTasks().size());

        // Reset Last Workflow Task to FAILED.
        Task lastTask =
                workflow.getTasks().stream()
                        .filter(t -> t.getReferenceTaskName().equals("task1_ref1"))
                        .collect(
                                groupingBy(
                                        Task::getReferenceTaskName,
                                        maxBy(comparingInt(Task::getSeq))))
                        .values()
                        .stream()
                        .map(Optional::get)
                        .collect(Collectors.toList())
                        .get(0);
        lastTask.setStatus(Status.FAILED);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        assertEquals(5, workflow.getTasks().size());

        // Reset Last Workflow Task to FAILED.
        // Reset Last Workflow Task to FAILED.
        Task lastTask2 =
                workflow.getTasks().stream()
                        .filter(t -> t.getReferenceTaskName().equals("task1_ref1"))
                        .collect(
                                groupingBy(
                                        Task::getReferenceTaskName,
                                        maxBy(comparingInt(Task::getSeq))))
                        .values()
                        .stream()
                        .map(Optional::get)
                        .collect(Collectors.toList())
                        .get(0);
        lastTask2.setStatus(Status.FAILED);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        assertEquals(6, workflow.getTasks().size());
    }

    @Test
    public void testRetryWorkflowWithJoinTask() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        Task forkTask = new Task();
        forkTask.setTaskType(TaskType.FORK_JOIN.toString());
        forkTask.setTaskId(UUID.randomUUID().toString());
        forkTask.setSeq(1);
        forkTask.setRetryCount(1);
        forkTask.setStatus(Status.COMPLETED);
        forkTask.setReferenceTaskName("task_fork");

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(1);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        Task joinTask = new Task();
        joinTask.setTaskType(TaskType.JOIN.toString());
        joinTask.setTaskId(UUID.randomUUID().toString());
        joinTask.setSeq(25);
        joinTask.setRetryCount(1);
        joinTask.setStatus(Status.CANCELED);
        joinTask.setReferenceTaskName("task_join");
        joinTask.getInputData()
                .put(
                        "joinOn",
                        Arrays.asList(
                                task_1_1.getReferenceTaskName(), task_2_1.getReferenceTaskName()));

        workflow.getTasks().addAll(Arrays.asList(forkTask, task_1_1, task_2_1, joinTask));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        assertEquals(6, workflow.getTasks().size());
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
    }

    @Test
    public void testRetryFromLastFailedSubWorkflowTaskThenStartWithLastFailedTask() {

        // given
        String id = IDGenerator.generate();
        String workflowInstanceId = IDGenerator.generate();
        Task task = new Task();
        task.setTaskType(TaskType.SIMPLE.name());
        task.setTaskDefName("task");
        task.setReferenceTaskName("task_ref");
        task.setWorkflowInstanceId(workflowInstanceId);
        task.setScheduledTime(System.currentTimeMillis());
        task.setTaskId(IDGenerator.generate());
        task.setStatus(Status.COMPLETED);
        task.setRetryCount(0);
        task.setWorkflowTask(new WorkflowTask());
        task.setOutputData(new HashMap<>());
        task.setSubWorkflowId(id);
        task.setSeq(1);

        Task task1 = new Task();
        task1.setTaskType(TaskType.SIMPLE.name());
        task1.setTaskDefName("task1");
        task1.setReferenceTaskName("task1_ref");
        task1.setWorkflowInstanceId(workflowInstanceId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(IDGenerator.generate());
        task1.setStatus(Status.FAILED);
        task1.setRetryCount(0);
        task1.setWorkflowTask(new WorkflowTask());
        task1.setOutputData(new HashMap<>());
        task1.setSubWorkflowId(id);
        task1.setSeq(2);

        Workflow subWorkflow = new Workflow();
        subWorkflow.setWorkflowId(id);
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("subworkflow");
        workflowDef.setVersion(1);
        subWorkflow.setWorkflowDefinition(workflowDef);
        subWorkflow.setStatus(Workflow.WorkflowStatus.FAILED);
        subWorkflow.getTasks().addAll(Arrays.asList(task, task1));
        subWorkflow.setParentWorkflowId("testRunWorkflowId");

        Task task2 = new Task();
        task2.setWorkflowInstanceId(subWorkflow.getWorkflowId());
        task2.setScheduledTime(System.currentTimeMillis());
        task2.setTaskId(IDGenerator.generate());
        task2.setStatus(Status.FAILED);
        task2.setRetryCount(0);
        task2.setOutputData(new HashMap<>());
        task2.setSubWorkflowId(id);
        task2.setTaskType(TaskType.SUB_WORKFLOW.name());

        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRunWorkflowId");
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);
        workflow.setTasks(Collections.singletonList(task2));
        workflowDef = new WorkflowDef();
        workflowDef.setName("first_workflow");
        workflow.setWorkflowDefinition(workflowDef);

        // when
        when(executionDAOFacade.getWorkflowById(workflow.getWorkflowId(), true))
                .thenReturn(workflow);
        when(executionDAOFacade.getWorkflowById(task.getSubWorkflowId(), true))
                .thenReturn(subWorkflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(workflowDef));
        when(executionDAOFacade.getTaskById(subWorkflow.getParentWorkflowTaskId()))
                .thenReturn(task1);
        when(executionDAOFacade.getWorkflowById(subWorkflow.getParentWorkflowId(), false))
                .thenReturn(workflow);

        workflowExecutor.retry(workflow.getWorkflowId(), true);

        // then
        assertEquals(task.getStatus(), Status.COMPLETED);
        assertEquals(task1.getStatus(), Status.IN_PROGRESS);
        assertEquals(workflow.getStatus(), WorkflowStatus.RUNNING);
        assertEquals(subWorkflow.getStatus(), WorkflowStatus.RUNNING);
    }

    @Test
    public void testRetryTimedOutWorkflowWithoutFailedTasks() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(WorkflowStatus.TIMED_OUT);

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(1);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.COMPLETED);
        task_1_1.setRetried(true);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.COMPLETED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_2_1));

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateWorkflowCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(
                        invocation -> {
                            updateTasksCalledCounter.incrementAndGet();
                            return null;
                        })
                .when(executionDAOFacade)
                .updateTasks(any());
        // end of setup

        // when
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));

        workflowExecutor.retry(workflow.getWorkflowId(), false);

        // then
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertTrue(workflow.getLastRetriedTime() > 0);
        assertEquals(1, updateWorkflowCalledCounter.get());
        assertEquals(1, updateTasksCalledCounter.get());
    }

    @Test
    public void testRerunWorkflow() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRerunWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRerunWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRerunWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);
        workflow.setReasonForIncompletion("task1 failed");
        workflow.setFailedReferenceTaskNames(
                new HashSet<String>() {
                    {
                        add("task1_ref1");
                    }
                });

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(1);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setRetried(true);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_2_1));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));
        RerunWorkflowRequest rerunWorkflowRequest = new RerunWorkflowRequest();
        rerunWorkflowRequest.setReRunFromWorkflowId(workflow.getWorkflowId());
        workflowExecutor.rerun(rerunWorkflowRequest);

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertEquals(null, workflow.getReasonForIncompletion());
        assertEquals(new HashSet<>(), workflow.getFailedReferenceTaskNames());
    }

    @Test
    public void testRerunSubWorkflow() {
        // setup
        String parentWorkflowId = IDGenerator.generate();
        String subWorkflowId = IDGenerator.generate();

        // sub workflow setup
        Task task1 = new Task();
        task1.setTaskType(TaskType.SIMPLE.name());
        task1.setTaskDefName("task1");
        task1.setReferenceTaskName("task1_ref");
        task1.setWorkflowInstanceId(subWorkflowId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(IDGenerator.generate());
        task1.setStatus(Status.COMPLETED);
        task1.setWorkflowTask(new WorkflowTask());
        task1.setOutputData(new HashMap<>());

        Task task2 = new Task();
        task2.setTaskType(TaskType.SIMPLE.name());
        task2.setTaskDefName("task2");
        task2.setReferenceTaskName("task2_ref");
        task2.setWorkflowInstanceId(subWorkflowId);
        task2.setScheduledTime(System.currentTimeMillis());
        task2.setTaskId(IDGenerator.generate());
        task2.setStatus(Status.COMPLETED);
        task2.setWorkflowTask(new WorkflowTask());
        task2.setOutputData(new HashMap<>());

        Workflow subWorkflow = new Workflow();
        subWorkflow.setParentWorkflowId(parentWorkflowId);
        subWorkflow.setWorkflowId(subWorkflowId);
        WorkflowDef subworkflowDef = new WorkflowDef();
        subworkflowDef.setName("subworkflow");
        subworkflowDef.setVersion(1);
        subWorkflow.setWorkflowDefinition(subworkflowDef);
        subWorkflow.setOwnerApp("junit_testRerunWorkflowId");
        subWorkflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        subWorkflow.getTasks().addAll(Arrays.asList(task1, task2));

        // parent workflow setup
        Task task = new Task();
        task.setWorkflowInstanceId(parentWorkflowId);
        task.setScheduledTime(System.currentTimeMillis());
        task.setTaskId(IDGenerator.generate());
        task.setStatus(Status.COMPLETED);
        task.setOutputData(new HashMap<>());
        task.setSubWorkflowId(subWorkflowId);
        task.setTaskType(TaskType.SUB_WORKFLOW.name());

        Workflow workflow = new Workflow();
        workflow.setWorkflowId(parentWorkflowId);
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("parentworkflow");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRerunWorkflowId");
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        workflow.getTasks().addAll(Arrays.asList(task));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(workflow.getWorkflowId(), true))
                .thenReturn(workflow);
        when(executionDAOFacade.getWorkflowById(task.getSubWorkflowId(), true))
                .thenReturn(subWorkflow);
        when(executionDAOFacade.getTaskById(subWorkflow.getParentWorkflowTaskId()))
                .thenReturn(task);
        when(executionDAOFacade.getWorkflowById(subWorkflow.getParentWorkflowId(), false))
                .thenReturn(workflow);

        RerunWorkflowRequest rerunWorkflowRequest = new RerunWorkflowRequest();
        rerunWorkflowRequest.setReRunFromWorkflowId(subWorkflow.getWorkflowId());
        workflowExecutor.rerun(rerunWorkflowRequest);

        // then:
        assertEquals(Status.IN_PROGRESS, task.getStatus());
        assertEquals(Workflow.WorkflowStatus.RUNNING, subWorkflow.getStatus());
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
    }

    @Test
    public void testRerunWorkflowWithTaskId() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRerunWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRerunWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);
        workflow.setReasonForIncompletion("task1 failed");
        workflow.setFailedReferenceTaskNames(
                new HashSet<String>() {
                    {
                        add("task1_ref1");
                    }
                });

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(1);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setRetried(true);
        task_1_1.setTaskDefName("task1");
        task_1_1.setWorkflowTask(new WorkflowTask());
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setWorkflowTask(new WorkflowTask());
        task_2_1.setReferenceTaskName("task2_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_2_1));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        when(metadataDAO.getWorkflowDef(anyString(), anyInt()))
                .thenReturn(Optional.of(new WorkflowDef()));
        RerunWorkflowRequest rerunWorkflowRequest = new RerunWorkflowRequest();
        rerunWorkflowRequest.setReRunFromWorkflowId(workflow.getWorkflowId());
        rerunWorkflowRequest.setReRunFromTaskId(task_1_1.getTaskId());
        workflowExecutor.rerun(rerunWorkflowRequest);

        // when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertNull(workflow.getReasonForIncompletion());
        assertEquals(new HashSet<>(), workflow.getFailedReferenceTaskNames());
    }

    @Test
    public void testRerunWorkflowWithSyncSystemTaskId() {
        // setup
        String workflowId = IDGenerator.generate();

        Task task1 = new Task();
        task1.setTaskType(TaskType.SIMPLE.name());
        task1.setTaskDefName("task1");
        task1.setReferenceTaskName("task1_ref");
        task1.setWorkflowInstanceId(workflowId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(IDGenerator.generate());
        task1.setStatus(Status.COMPLETED);
        task1.setWorkflowTask(new WorkflowTask());
        task1.setOutputData(new HashMap<>());

        Task task2 = new Task();
        task2.setTaskType(TaskType.JSON_JQ_TRANSFORM.name());
        task2.setReferenceTaskName("task2_ref");
        task2.setWorkflowInstanceId(workflowId);
        task2.setScheduledTime(System.currentTimeMillis());
        task2.setTaskId("system-task-id");
        task2.setStatus(Status.FAILED);

        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("workflow");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRerunWorkflowId");
        workflow.setStatus(WorkflowStatus.FAILED);
        workflow.setReasonForIncompletion("task2 failed");
        workflow.setFailedReferenceTaskNames(
                new HashSet<String>() {
                    {
                        add("task2_ref");
                    }
                });
        workflow.getTasks().addAll(Arrays.asList(task1, task2));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(workflow.getWorkflowId(), true))
                .thenReturn(workflow);
        RerunWorkflowRequest rerunWorkflowRequest = new RerunWorkflowRequest();
        rerunWorkflowRequest.setReRunFromWorkflowId(workflow.getWorkflowId());
        rerunWorkflowRequest.setReRunFromTaskId(task2.getTaskId());
        workflowExecutor.rerun(rerunWorkflowRequest);

        // then:
        assertEquals(Status.COMPLETED, task2.getStatus());
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertNull(workflow.getReasonForIncompletion());
        assertEquals(new HashSet<>(), workflow.getFailedReferenceTaskNames());
    }

    @Test
    public void testRerunSubWorkflowWithTaskId() {
        // setup
        String parentWorkflowId = IDGenerator.generate();
        String subWorkflowId = IDGenerator.generate();

        // sub workflow setup
        Task task1 = new Task();
        task1.setTaskType(TaskType.SIMPLE.name());
        task1.setTaskDefName("task1");
        task1.setReferenceTaskName("task1_ref");
        task1.setWorkflowInstanceId(subWorkflowId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(IDGenerator.generate());
        task1.setStatus(Status.COMPLETED);
        task1.setWorkflowTask(new WorkflowTask());
        task1.setOutputData(new HashMap<>());

        Task task2 = new Task();
        task2.setTaskType(TaskType.SIMPLE.name());
        task2.setTaskDefName("task2");
        task2.setReferenceTaskName("task2_ref");
        task2.setWorkflowInstanceId(subWorkflowId);
        task2.setScheduledTime(System.currentTimeMillis());
        task2.setTaskId(IDGenerator.generate());
        task2.setStatus(Status.COMPLETED);
        task2.setWorkflowTask(new WorkflowTask());
        task2.setOutputData(new HashMap<>());

        Workflow subWorkflow = new Workflow();
        subWorkflow.setParentWorkflowId(parentWorkflowId);
        subWorkflow.setWorkflowId(subWorkflowId);
        WorkflowDef subworkflowDef = new WorkflowDef();
        subworkflowDef.setName("subworkflow");
        subworkflowDef.setVersion(1);
        subWorkflow.setWorkflowDefinition(subworkflowDef);
        subWorkflow.setOwnerApp("junit_testRerunWorkflowId");
        subWorkflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        subWorkflow.getTasks().addAll(Arrays.asList(task1, task2));

        // parent workflow setup
        Task task = new Task();
        task.setWorkflowInstanceId(parentWorkflowId);
        task.setScheduledTime(System.currentTimeMillis());
        task.setTaskId(IDGenerator.generate());
        task.setStatus(Status.COMPLETED);
        task.setOutputData(new HashMap<>());
        task.setSubWorkflowId(subWorkflowId);
        task.setTaskType(TaskType.SUB_WORKFLOW.name());

        Workflow workflow = new Workflow();
        workflow.setWorkflowId(parentWorkflowId);
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("parentworkflow");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRerunWorkflowId");
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        workflow.getTasks().addAll(Arrays.asList(task));
        // end of setup

        // when:
        when(executionDAOFacade.getWorkflowById(workflow.getWorkflowId(), true))
                .thenReturn(workflow);
        when(executionDAOFacade.getWorkflowById(task.getSubWorkflowId(), true))
                .thenReturn(subWorkflow);
        when(executionDAOFacade.getTaskById(subWorkflow.getParentWorkflowTaskId()))
                .thenReturn(task);
        when(executionDAOFacade.getWorkflowById(subWorkflow.getParentWorkflowId(), false))
                .thenReturn(workflow);

        RerunWorkflowRequest rerunWorkflowRequest = new RerunWorkflowRequest();
        rerunWorkflowRequest.setReRunFromWorkflowId(subWorkflow.getWorkflowId());
        rerunWorkflowRequest.setReRunFromTaskId(task2.getTaskId());
        workflowExecutor.rerun(rerunWorkflowRequest);

        // then:
        assertEquals(Status.SCHEDULED, task2.getStatus());
        assertEquals(Status.IN_PROGRESS, task.getStatus());
        assertEquals(Workflow.WorkflowStatus.RUNNING, subWorkflow.getStatus());
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
    }

    @Test
    public void testGetActiveDomain() {
        String taskType = "test-task";
        String[] domains = new String[] {"domain1", "domain2"};

        PollData pollData1 =
                new PollData(
                        "queue1", domains[0], "worker1", System.currentTimeMillis() - 99 * 1000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[0]))
                .thenReturn(pollData1);
        String activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(domains[0], activeDomain);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        PollData pollData2 =
                new PollData(
                        "queue2", domains[1], "worker2", System.currentTimeMillis() - 99 * 1000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[1]))
                .thenReturn(pollData2);
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(domains[1], activeDomain);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(domains[1], activeDomain);

        domains = new String[] {""};
        when(executionDAOFacade.getTaskPollDataByDomain(any(), any())).thenReturn(new PollData());
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertNotNull(activeDomain);
        assertEquals("", activeDomain);

        domains = new String[] {};
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertNull(activeDomain);

        activeDomain = workflowExecutor.getActiveDomain(taskType, null);
        assertNull(activeDomain);

        domains = new String[] {"test-domain"};
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), anyString())).thenReturn(null);
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertNotNull(activeDomain);
        assertEquals("test-domain", activeDomain);
    }

    @Test
    public void testInactiveDomains() {
        String taskType = "test-task";
        String[] domains = new String[] {"domain1", "domain2"};

        PollData pollData1 =
                new PollData(
                        "queue1", domains[0], "worker1", System.currentTimeMillis() - 99 * 10000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[0]))
                .thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[1])).thenReturn(null);
        String activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals("domain2", activeDomain);
    }

    @Test
    public void testDefaultDomain() {
        String taskType = "test-task";
        String[] domains = new String[] {"domain1", "domain2", "NO_DOMAIN"};

        PollData pollData1 =
                new PollData(
                        "queue1", domains[0], "worker1", System.currentTimeMillis() - 99 * 10000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[0]))
                .thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[1])).thenReturn(null);
        String activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertNull(activeDomain);
    }

    @Test
    public void testTaskToDomain() {
        Workflow workflow = generateSampleWorkflow();
        List<Task> tasks = generateSampleTasks(3);

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "mydomain");
        workflow.setTaskToDomain(taskToDomain);

        PollData pollData1 =
                new PollData(
                        "queue1", "mydomain", "worker1", System.currentTimeMillis() - 99 * 100);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), anyString()))
                .thenReturn(pollData1);
        workflowExecutor.setTaskDomains(tasks, workflow);

        assertNotNull(tasks);
        tasks.forEach(task -> assertEquals("mydomain", task.getDomain()));
    }

    @Test
    public void testTaskToDomainsPerTask() {
        Workflow workflow = generateSampleWorkflow();
        List<Task> tasks = generateSampleTasks(2);

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "mydomain, NO_DOMAIN");
        workflow.setTaskToDomain(taskToDomain);

        PollData pollData1 =
                new PollData(
                        "queue1", "mydomain", "worker1", System.currentTimeMillis() - 99 * 100);
        when(executionDAOFacade.getTaskPollDataByDomain(eq("task1"), anyString()))
                .thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(eq("task2"), anyString())).thenReturn(null);
        workflowExecutor.setTaskDomains(tasks, workflow);

        assertEquals("mydomain", tasks.get(0).getDomain());
        assertNull(tasks.get(1).getDomain());
    }

    @Test
    public void testTaskToDomainOverrides() {
        Workflow workflow = generateSampleWorkflow();
        List<Task> tasks = generateSampleTasks(4);

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "mydomain");
        taskToDomain.put("task2", "someInactiveDomain, NO_DOMAIN");
        taskToDomain.put("task3", "someActiveDomain, NO_DOMAIN");
        taskToDomain.put("task4", "someInactiveDomain, someInactiveDomain2");
        workflow.setTaskToDomain(taskToDomain);

        PollData pollData1 =
                new PollData(
                        "queue1", "mydomain", "worker1", System.currentTimeMillis() - 99 * 100);
        PollData pollData2 =
                new PollData(
                        "queue2",
                        "someActiveDomain",
                        "worker2",
                        System.currentTimeMillis() - 99 * 100);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("mydomain")))
                .thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("someInactiveDomain")))
                .thenReturn(null);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("someActiveDomain")))
                .thenReturn(pollData2);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("someInactiveDomain")))
                .thenReturn(null);
        workflowExecutor.setTaskDomains(tasks, workflow);

        assertEquals("mydomain", tasks.get(0).getDomain());
        assertNull(tasks.get(1).getDomain());
        assertEquals("someActiveDomain", tasks.get(2).getDomain());
        assertEquals("someInactiveDomain2", tasks.get(3).getDomain());
    }

    @Test
    public void testDedupAndAddTasks() {
        Workflow workflow = new Workflow();

        Task task1 = new Task();
        task1.setReferenceTaskName("task1");
        task1.setRetryCount(1);

        Task task2 = new Task();
        task2.setReferenceTaskName("task2");
        task2.setRetryCount(2);

        List<Task> tasks = new ArrayList<>(Arrays.asList(task1, task2));

        List<Task> taskList = workflowExecutor.dedupAndAddTasks(workflow, tasks);
        assertEquals(2, taskList.size());
        assertEquals(tasks, taskList);
        assertEquals(workflow.getTasks(), taskList);

        // Adding the same tasks again
        taskList = workflowExecutor.dedupAndAddTasks(workflow, tasks);
        assertEquals(0, taskList.size());
        assertEquals(workflow.getTasks(), tasks);

        // Adding 2 new tasks
        Task newTask = new Task();
        newTask.setReferenceTaskName("newTask");
        newTask.setRetryCount(0);

        taskList = workflowExecutor.dedupAndAddTasks(workflow, Collections.singletonList(newTask));
        assertEquals(1, taskList.size());
        assertEquals(newTask, taskList.get(0));
        assertEquals(3, workflow.getTasks().size());
    }

    @Test(expected = ApplicationException.class)
    public void testTerminateCompletedWorkflow() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testTerminateTerminalWorkflow");
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.terminateWorkflow(
                workflow.getWorkflowId(), "test terminating terminal workflow");
    }

    @Test
    public void testResetCallbacksForWorkflowTasks() {
        String workflowId = "test-workflow-id";
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setStatus(WorkflowStatus.RUNNING);

        Task completedTask = new Task();
        completedTask.setTaskType(TaskType.SIMPLE.name());
        completedTask.setReferenceTaskName("completedTask");
        completedTask.setWorkflowInstanceId(workflowId);
        completedTask.setScheduledTime(System.currentTimeMillis());
        completedTask.setCallbackAfterSeconds(300);
        completedTask.setTaskId("simple-task-id");
        completedTask.setStatus(Status.COMPLETED);

        Task systemTask = new Task();
        systemTask.setTaskType(TaskType.WAIT.name());
        systemTask.setReferenceTaskName("waitTask");
        systemTask.setWorkflowInstanceId(workflowId);
        systemTask.setScheduledTime(System.currentTimeMillis());
        systemTask.setTaskId("system-task-id");
        systemTask.setStatus(Status.SCHEDULED);

        Task simpleTask = new Task();
        simpleTask.setTaskType(TaskType.SIMPLE.name());
        simpleTask.setReferenceTaskName("simpleTask");
        simpleTask.setWorkflowInstanceId(workflowId);
        simpleTask.setScheduledTime(System.currentTimeMillis());
        simpleTask.setCallbackAfterSeconds(300);
        simpleTask.setTaskId("simple-task-id");
        simpleTask.setStatus(Status.SCHEDULED);

        Task noCallbackTask = new Task();
        noCallbackTask.setTaskType(TaskType.SIMPLE.name());
        noCallbackTask.setReferenceTaskName("noCallbackTask");
        noCallbackTask.setWorkflowInstanceId(workflowId);
        noCallbackTask.setScheduledTime(System.currentTimeMillis());
        noCallbackTask.setCallbackAfterSeconds(0);
        noCallbackTask.setTaskId("no-callback-task-id");
        noCallbackTask.setStatus(Status.SCHEDULED);

        workflow.getTasks()
                .addAll(Arrays.asList(completedTask, systemTask, simpleTask, noCallbackTask));
        when(executionDAOFacade.getWorkflowById(workflowId, true)).thenReturn(workflow);

        workflowExecutor.resetCallbacksForWorkflow(workflowId);
        verify(queueDAO, times(1)).resetOffsetTime(anyString(), anyString());
    }

    @Test
    public void testUpdateParentWorkflowTask() {
        SubWorkflow subWf = new SubWorkflow(objectMapper);
        String parentWorkflowTaskId = "parent_workflow_task_id";
        String workflowId = "workflow_id";

        Workflow subWorkflow = new Workflow();
        subWorkflow.setWorkflowId(workflowId);
        subWorkflow.setParentWorkflowTaskId(parentWorkflowTaskId);
        subWorkflow.setStatus(WorkflowStatus.COMPLETED);

        Task subWorkflowTask = new Task();
        subWorkflowTask.setSubWorkflowId(workflowId);
        subWorkflowTask.setStatus(Status.IN_PROGRESS);
        subWorkflowTask.setExternalOutputPayloadStoragePath(null);

        when(executionDAOFacade.getTaskById(parentWorkflowTaskId)).thenReturn(subWorkflowTask);
        when(executionDAOFacade.getWorkflowById(workflowId, false)).thenReturn(subWorkflow);

        workflowExecutor.updateParentWorkflowTask(subWorkflow);
        ArgumentCaptor<Task> argumentCaptor = ArgumentCaptor.forClass(Task.class);
        verify(executionDAOFacade, times(1)).updateTask(argumentCaptor.capture());
        assertEquals(Status.COMPLETED, argumentCaptor.getAllValues().get(0).getStatus());
        assertEquals(workflowId, argumentCaptor.getAllValues().get(0).getSubWorkflowId());
    }

    @Test
    public void testStartWorkflow() {
        WorkflowDef def = new WorkflowDef();
        def.setName("test");
        Workflow workflow = new Workflow();
        workflow.setWorkflowDefinition(def);

        Map<String, Object> workflowInput = new HashMap<>();
        String externalInputPayloadStoragePath = null;
        String correlationId = null;
        Integer priority = null;
        String parentWorkflowId = null;
        String parentWorkflowTaskId = null;
        String event = null;
        Map<String, String> taskToDomain = null;

        when(executionLockService.acquireLock(anyString())).thenReturn(true);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.startWorkflow(
                def,
                workflowInput,
                externalInputPayloadStoragePath,
                correlationId,
                priority,
                parentWorkflowId,
                parentWorkflowTaskId,
                event,
                taskToDomain);

        verify(executionDAOFacade, times(1)).createWorkflow(any(Workflow.class));
        verify(executionLockService, times(2)).acquireLock(anyString());
        verify(executionDAOFacade, times(1)).getWorkflowById(anyString(), anyBoolean());
    }

    @Test
    public void testScheduleNextIteration() {
        Workflow workflow = generateSampleWorkflow();
        workflow.setTaskToDomain(
                new HashMap<String, String>() {
                    {
                        put("TEST", "domain1");
                    }
                });
        Task loopTask = mock(Task.class);
        WorkflowTask loopWfTask = mock(WorkflowTask.class);
        when(loopTask.getWorkflowTask()).thenReturn(loopWfTask);
        List<WorkflowTask> loopOver =
                new ArrayList<WorkflowTask>() {
                    {
                        WorkflowTask workflowTask = new WorkflowTask();
                        workflowTask.setType(TaskType.TASK_TYPE_SIMPLE);
                        workflowTask.setName("TEST");
                        workflowTask.setTaskDefinition(new TaskDef());
                        add(workflowTask);
                    }
                };
        when(loopWfTask.getLoopOver()).thenReturn(loopOver);

        workflowExecutor.scheduleNextIteration(loopTask, workflow);
        verify(executionDAOFacade).getTaskPollDataByDomain("TEST", "domain1");
    }

    @Test
    public void testCancelNonTerminalTasks() {
        WorkflowDef def = new WorkflowDef();
        def.setWorkflowStatusListenerEnabled(true);

        Workflow workflow = generateSampleWorkflow();
        workflow.setWorkflowDefinition(def);

        Task subWorkflowTask = new Task();
        subWorkflowTask.setTaskId(UUID.randomUUID().toString());
        subWorkflowTask.setTaskType(TaskType.SUB_WORKFLOW.name());
        subWorkflowTask.setStatus(Status.IN_PROGRESS);

        Task lambdaTask = new Task();
        lambdaTask.setTaskId(UUID.randomUUID().toString());
        lambdaTask.setTaskType(TaskType.LAMBDA.name());
        lambdaTask.setStatus(Status.SCHEDULED);

        Task simpleTask = new Task();
        simpleTask.setTaskId(UUID.randomUUID().toString());
        simpleTask.setTaskType(TaskType.SIMPLE.name());
        simpleTask.setStatus(Status.COMPLETED);

        workflow.getTasks().addAll(Arrays.asList(subWorkflowTask, lambdaTask, simpleTask));

        List<String> erroredTasks = workflowExecutor.cancelNonTerminalTasks(workflow);
        assertTrue(erroredTasks.isEmpty());
        ArgumentCaptor<Task> argumentCaptor = ArgumentCaptor.forClass(Task.class);
        verify(executionDAOFacade, times(2)).updateTask(argumentCaptor.capture());
        assertEquals(2, argumentCaptor.getAllValues().size());
        assertEquals(
                TaskType.SUB_WORKFLOW.name(), argumentCaptor.getAllValues().get(0).getTaskType());
        assertEquals(Status.CANCELED, argumentCaptor.getAllValues().get(0).getStatus());
        assertEquals(TaskType.LAMBDA.name(), argumentCaptor.getAllValues().get(1).getTaskType());
        assertEquals(Status.CANCELED, argumentCaptor.getAllValues().get(1).getStatus());
        verify(workflowStatusListener, times(1)).onWorkflowFinalizedIfEnabled(any(Workflow.class));
    }

    @Test
    public void testPauseWorkflow() {
        when(executionLockService.acquireLock(anyString(), anyLong())).thenReturn(true);
        doNothing().when(executionLockService).releaseLock(anyString());

        String workflowId = "testPauseWorkflowId";
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);

        // if workflow is in terminal state
        workflow.setStatus(COMPLETED);
        when(executionDAOFacade.getWorkflowById(workflowId, false)).thenReturn(workflow);
        try {
            workflowExecutor.pauseWorkflow(workflowId);
            fail("Expected " + ApplicationException.class);
        } catch (ApplicationException e) {
            assertEquals(e.getCode(), CONFLICT);
            verify(executionDAOFacade, never()).updateWorkflow(any(Workflow.class));
            verify(queueDAO, never()).remove(anyString(), anyString());
        }

        // if workflow is already PAUSED
        workflow.setStatus(PAUSED);
        when(executionDAOFacade.getWorkflowById(workflowId, false)).thenReturn(workflow);
        workflowExecutor.pauseWorkflow(workflowId);
        assertEquals(PAUSED, workflow.getStatus());
        verify(executionDAOFacade, never()).updateWorkflow(any(Workflow.class));
        verify(queueDAO, never()).remove(anyString(), anyString());

        // if workflow is RUNNING
        workflow.setStatus(RUNNING);
        when(executionDAOFacade.getWorkflowById(workflowId, false)).thenReturn(workflow);
        workflowExecutor.pauseWorkflow(workflowId);
        assertEquals(PAUSED, workflow.getStatus());
        verify(executionDAOFacade, times(1)).updateWorkflow(any(Workflow.class));
        verify(queueDAO, times(1)).remove(anyString(), anyString());
    }

    @Test
    public void testResumeWorkflow() {
        String workflowId = "testResumeWorkflowId";
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);

        // if workflow is not in PAUSED state
        workflow.setStatus(COMPLETED);
        when(executionDAOFacade.getWorkflowById(workflowId, false)).thenReturn(workflow);
        try {
            workflowExecutor.resumeWorkflow(workflowId);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            verify(executionDAOFacade, never()).updateWorkflow(any(Workflow.class));
            verify(queueDAO, never()).push(anyString(), anyString(), anyInt(), anyLong());
        }

        // if workflow is in PAUSED state
        workflow.setStatus(PAUSED);
        when(executionDAOFacade.getWorkflowById(workflowId, false)).thenReturn(workflow);
        workflowExecutor.resumeWorkflow(workflowId);
        assertEquals(RUNNING, workflow.getStatus());
        assertTrue(workflow.getLastRetriedTime() > 0);
        verify(executionDAOFacade, times(1)).updateWorkflow(any(Workflow.class));
        verify(queueDAO, times(1)).push(anyString(), anyString(), anyInt(), anyLong());
    }

    private Workflow generateSampleWorkflow() {
        // setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testRetryWorkflowId");
        workflowDef.setVersion(1);
        workflow.setWorkflowDefinition(workflowDef);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        return workflow;
    }

    private List<Task> generateSampleTasks(int count) {
        if (count == 0) {
            return null;
        }
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Task task = new Task();
            task.setTaskId(UUID.randomUUID().toString());
            task.setSeq(i);
            task.setRetryCount(1);
            task.setTaskType("task" + (i + 1));
            task.setStatus(Status.COMPLETED);
            task.setTaskDefName("taskX");
            task.setReferenceTaskName("task_ref" + (i + 1));
            tasks.add(task);
        }

        return tasks;
    }
}

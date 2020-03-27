/*
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.core.execution;

import static com.netflix.conductor.core.execution.tasks.SubWorkflow.SUB_WORKFLOW_ID;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.maxBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.execution.mapper.DecisionTaskMapper;
import com.netflix.conductor.core.execution.mapper.DynamicTaskMapper;
import com.netflix.conductor.core.execution.mapper.EventTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinDynamicTaskMapper;
import com.netflix.conductor.core.execution.mapper.ForkJoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.HTTPTaskMapper;
import com.netflix.conductor.core.execution.mapper.JoinTaskMapper;
import com.netflix.conductor.core.execution.mapper.LambdaTaskMapper;
import com.netflix.conductor.core.execution.mapper.SimpleTaskMapper;
import com.netflix.conductor.core.execution.mapper.SubWorkflowTaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.UserDefinedTaskMapper;
import com.netflix.conductor.core.execution.mapper.WaitTaskMapper;
import com.netflix.conductor.core.execution.tasks.Terminate;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionLockService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

/**
 * @author Viren
 */
public class TestWorkflowExecutor {

    private WorkflowExecutor workflowExecutor;
    private ExecutionDAOFacade executionDAOFacade;
    private MetadataDAO metadataDAO;
    private QueueDAO queueDAO;
    private WorkflowStatusListener workflowStatusListener;
    private ExecutionLockService executionLockService;

    @Before
    public void init() {
        TestConfiguration config = new TestConfiguration();
        executionDAOFacade = mock(ExecutionDAOFacade.class);
        metadataDAO = mock(MetadataDAO.class);
        queueDAO = mock(QueueDAO.class);
        workflowStatusListener = mock(WorkflowStatusListener.class);
        ExternalPayloadStorageUtils externalPayloadStorageUtils = mock(ExternalPayloadStorageUtils.class);
        executionLockService = mock(ExecutionLockService.class);
        ObjectMapper objectMapper = new JsonMapperProvider().get();
        ParametersUtils parametersUtils = new ParametersUtils();
        Map<String, TaskMapper> taskMappers = new HashMap<>();
        taskMappers.put("DECISION", new DecisionTaskMapper());
        taskMappers.put("DYNAMIC", new DynamicTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("FORK_JOIN", new ForkJoinTaskMapper());
        taskMappers.put("JOIN", new JoinTaskMapper());
        taskMappers.put("FORK_JOIN_DYNAMIC", new ForkJoinDynamicTaskMapper(parametersUtils, objectMapper, metadataDAO));
        taskMappers.put("USER_DEFINED", new UserDefinedTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("SIMPLE", new SimpleTaskMapper(parametersUtils));
        taskMappers.put("SUB_WORKFLOW", new SubWorkflowTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("EVENT", new EventTaskMapper(parametersUtils));
        taskMappers.put("WAIT", new WaitTaskMapper(parametersUtils));
        taskMappers.put("HTTP", new HTTPTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("LAMBDA", new LambdaTaskMapper(parametersUtils));

        DeciderService deciderService = new DeciderService(parametersUtils, metadataDAO, externalPayloadStorageUtils, taskMappers, config);
        MetadataMapperService metadataMapperService = new MetadataMapperService(metadataDAO);
        workflowExecutor = new WorkflowExecutor(deciderService, metadataDAO, queueDAO, metadataMapperService, workflowStatusListener, executionDAOFacade, config, executionLockService);
    }

    @Test
    public void testScheduleTask() {

        AtomicBoolean httpTaskExecuted = new AtomicBoolean(false);
        AtomicBoolean http2TaskExecuted = new AtomicBoolean(false);

        new Wait();
        new WorkflowSystemTask("HTTP") {
            @Override
            public boolean isAsync() {
                return true;
            }

            @Override
            public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
                httpTaskExecuted.set(true);
                task.setStatus(Status.COMPLETED);
                super.start(workflow, task, executor);
            }
        };

        new WorkflowSystemTask("HTTP2") {

            @Override
            public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
                http2TaskExecuted.set(true);
                task.setStatus(Status.COMPLETED);
                super.start(workflow, task, executor);
            }
        };

        Workflow workflow = new Workflow();
        workflow.setWorkflowId("1");

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
        task2.setTaskType(Wait.NAME);
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
        doAnswer(invocation -> {
            startedTaskCount.incrementAndGet();
            return null;
        }).when(executionDAOFacade)
                .updateTask(any());

        AtomicInteger queuedTaskCount = new AtomicInteger(0);
        final Answer answer = invocation -> {
            String queueName = invocation.getArgument(0, String.class);
            System.out.println(queueName);
            queuedTaskCount.incrementAndGet();
            return null;
        };
        doAnswer(answer).when(queueDAO).push(any(), any(), anyLong());
        doAnswer(answer).when(queueDAO).push(any(), any(), anyInt(), anyLong());

        boolean stateChanged = workflowExecutor.scheduleTask(workflow, tasks);
        assertEquals(2, startedTaskCount.get());
        assertEquals(1, queuedTaskCount.get());
        assertTrue(stateChanged);
        assertFalse(httpTaskExecuted.get());
        assertTrue(http2TaskExecuted.get());
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
        doAnswer(invocation -> {
            updateWorkflowCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            updateTasksCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateTasks(any());

        AtomicInteger removeQueueEntryCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            removeQueueEntryCalledCounter.incrementAndGet();
            return null;
        }).when(queueDAO).remove(anyString(), anyString());

        workflowExecutor.completeWorkflow(workflow);
        assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
        assertEquals(2, updateWorkflowCalledCounter.get());
        assertEquals(1, updateTasksCalledCounter.get());
        assertEquals(1, removeQueueEntryCalledCounter.get());


        def.setWorkflowStatusListenerEnabled(true);
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflowExecutor.completeWorkflow(workflow);
        verify(workflowStatusListener, times(2)).onWorkflowCompleted(any(Workflow.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTerminatedWorkflow() {
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
        doAnswer(invocation -> {
            updateWorkflowCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            updateTasksCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateTasks(any());

        AtomicInteger removeQueueEntryCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            removeQueueEntryCalledCounter.incrementAndGet();
            return null;
        }).when(queueDAO).remove(anyString(), anyString());

        workflowExecutor.terminateWorkflow("workflowId", "reason");
        assertEquals(Workflow.WorkflowStatus.TERMINATED, workflow.getStatus());
        assertEquals(2, updateWorkflowCalledCounter.get());
        assertEquals(1, removeQueueEntryCalledCounter.get());


        def.setWorkflowStatusListenerEnabled(true);
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
        workflowExecutor.completeWorkflow(workflow);
        verify(workflowStatusListener, times(1)).onWorkflowCompleted(any(Workflow.class));
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

        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        doNothing().when(executionDAOFacade).removeTask(any());
        when(metadataDAO.get(workflow.getWorkflowName(), workflow.getWorkflowVersion())).thenReturn(Optional.of(workflowDef));
        when(metadataDAO.getTaskDef(workflowTask.getName())).thenReturn(new TaskDef());
        when(executionDAOFacade.updateWorkflow(any())).thenReturn("");

        workflowExecutor.rewind(workflow.getWorkflowId(), false);
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        verify(metadataDAO, never()).getLatest(any());

        ArgumentCaptor<Workflow> argumentCaptor = ArgumentCaptor.forClass(Workflow.class);
        verify(executionDAOFacade, times(1)).createWorkflow(argumentCaptor.capture());
        assertEquals(workflow.getWorkflowId(), argumentCaptor.getAllValues().get(0).getWorkflowId());
        assertEquals(workflow.getWorkflowDefinition(), argumentCaptor.getAllValues().get(0).getWorkflowDefinition());

        // add a new version of the workflow definition and restart with latest
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        workflowDef = new WorkflowDef();
        workflowDef.setName("testDef");
        workflowDef.setVersion(2);
        workflowDef.setRestartable(true);
        workflowDef.getTasks().addAll(Collections.singletonList(workflowTask));

        when(metadataDAO.getLatest(workflow.getWorkflowName())).thenReturn(Optional.of(workflowDef));
        workflowExecutor.rewind(workflow.getWorkflowId(), true);
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        verify(metadataDAO, times(1)).getLatest(anyString());

        argumentCaptor = ArgumentCaptor.forClass(Workflow.class);
        verify(executionDAOFacade, times(2)).createWorkflow(argumentCaptor.capture());
        assertEquals(workflow.getWorkflowId(), argumentCaptor.getAllValues().get(1).getWorkflowId());
        assertEquals(workflowDef, argumentCaptor.getAllValues().get(1).getWorkflowDefinition());
    }


    @Test(expected = ApplicationException.class)
    public void testRetryNonTerminalWorkflow() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryNonTerminalWorkflow");
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.retry(workflow.getWorkflowId());

    }

    @Test(expected = ApplicationException.class)
    public void testRetryWorkflowNoTasks() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("ApplicationException");
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);
        //noinspection unchecked
        workflow.setTasks(new ArrayList());
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.retry(workflow.getWorkflowId());
    }

    @Test(expected = ApplicationException.class)
    public void testRetryWorkflowNoFailedTasks() {
        //setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        workflow.setWorkflowType("testRetryWorkflowId");
        workflow.setVersion(1);
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
        //end of setup

        //when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        WorkflowDef workflowDef = new WorkflowDef();
        when(metadataDAO.get(anyString(), anyInt())).thenReturn(Optional.of(workflowDef));

        workflowExecutor.retry(workflow.getWorkflowId());
    }

    @Test
    public void testRetryWorkflow() {
        //setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        workflow.setWorkflowType("testRetryWorkflowId");
        workflow.setVersion(1);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        AtomicInteger updateWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            updateWorkflowCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateWorkflow(any());

        AtomicInteger updateTasksCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            updateTasksCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateTasks(any());

        AtomicInteger updateTaskCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            updateTaskCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).updateTask(any());

        // add 2 failed task in 2 forks and 1 cancelled in the 3rd fork
        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(1);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.CANCELED);
        task_1_1.setRetried(true);
        task_1_1.setTaskDefName("task1");
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_1_2 = new Task();
        task_1_2.setTaskId(UUID.randomUUID().toString());
        task_1_2.setSeq(21);
        task_1_2.setRetryCount(1);
        task_1_2.setTaskType(TaskType.SIMPLE.toString());
        task_1_2.setStatus(Status.FAILED);
        task_1_2.setTaskDefName("task1");
        task_1_2.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.FAILED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setReferenceTaskName("task2_ref1");


        Task task_3_1 = new Task();
        task_3_1.setTaskId(UUID.randomUUID().toString());
        task_3_1.setSeq(23);
        task_3_1.setRetryCount(1);
        task_3_1.setStatus(Status.CANCELED);
        task_3_1.setTaskType(TaskType.SIMPLE.toString());
        task_3_1.setTaskDefName("task3");
        task_3_1.setReferenceTaskName("task3_ref1");

        Task task_4_1 = new Task();
        task_4_1.setTaskId(UUID.randomUUID().toString());
        task_4_1.setSeq(122);
        task_4_1.setRetryCount(1);
        task_4_1.setStatus(Status.FAILED);
        task_4_1.setTaskType(TaskType.SIMPLE.toString());
        task_4_1.setTaskDefName("task1");
        task_4_1.setReferenceTaskName("task4_refABC");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_1_2, task_2_1, task_3_1, task_4_1));
        //end of setup

        //when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        WorkflowDef workflowDef = new WorkflowDef();
        when(metadataDAO.get(anyString(), anyInt())).thenReturn(Optional.of(workflowDef));

        workflowExecutor.retry(workflow.getWorkflowId());

        //when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
        assertEquals(1, updateWorkflowCalledCounter.get());
        assertEquals(1, updateTasksCalledCounter.get());
        assertEquals(0, updateTaskCalledCounter.get());
    }

    @Test
    public void testRetryWorkflowReturnsNoDuplicates() {
        //setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        workflow.setWorkflowType("testRetryWorkflowId");
        workflow.setVersion(1);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(0);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setTaskDefName("task1");
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_1_2 = new Task();
        task_1_2.setTaskId(UUID.randomUUID().toString());
        task_1_2.setSeq(21);
        task_1_2.setRetryCount(1);
        task_1_2.setTaskType(TaskType.SIMPLE.toString());
        task_1_2.setStatus(Status.COMPLETED);
        task_1_2.setTaskDefName("task1");
        task_1_2.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(0);
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setReferenceTaskName("task2_ref1");

        Task task_3_1 = new Task();
        task_3_1.setTaskId(UUID.randomUUID().toString());
        task_3_1.setSeq(51);
        task_3_1.setRetryCount(1);
        task_3_1.setStatus(Status.FAILED_WITH_TERMINAL_ERROR);
        task_3_1.setTaskType(TaskType.SIMPLE.toString());
        task_3_1.setTaskDefName("task1");
        task_3_1.setReferenceTaskName("task3_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_1_2, task_2_1, task_3_1));
        //end of setup

        //when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        WorkflowDef workflowDef = new WorkflowDef();
        when(metadataDAO.get(anyString(), anyInt())).thenReturn(Optional.of(workflowDef));

        workflowExecutor.retry(workflow.getWorkflowId());

        assertEquals(6, workflow.getTasks().size());
    }


    @Test
    public void testRetryWorkflowMultipleRetries() {
        //setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        workflow.setWorkflowType("testRetryWorkflowId");
        workflow.setVersion(1);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        Task task_1_1 = new Task();
        task_1_1.setTaskId(UUID.randomUUID().toString());
        task_1_1.setSeq(20);
        task_1_1.setRetryCount(0);
        task_1_1.setTaskType(TaskType.SIMPLE.toString());
        task_1_1.setStatus(Status.FAILED);
        task_1_1.setTaskDefName("task1");
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(20);
        task_2_1.setRetryCount(0);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskDefName("task1");
        task_2_1.setReferenceTaskName("task2_ref1");

        workflow.getTasks().addAll(Arrays.asList(task_1_1, task_2_1));
        //end of setup

        //when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        WorkflowDef workflowDef = new WorkflowDef();
        when(metadataDAO.get(anyString(), anyInt())).thenReturn(Optional.of(workflowDef));

        workflowExecutor.retry(workflow.getWorkflowId());

        assertEquals(4, workflow.getTasks().size());

        // Reset Last Workflow Task to FAILED.
        Task lastTask = workflow.getTasks().stream()
                .filter(t -> t.getReferenceTaskName().equals("task1_ref1"))
                .collect(groupingBy(Task::getReferenceTaskName, maxBy(comparingInt(Task::getSeq)))).values()
                .stream().map(Optional::get)
                .collect(Collectors.toList()).get(0);
        lastTask.setStatus(Status.FAILED);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        workflowExecutor.retry(workflow.getWorkflowId());

        assertEquals(5, workflow.getTasks().size());

        // Reset Last Workflow Task to FAILED.
        // Reset Last Workflow Task to FAILED.
        Task lastTask2 = workflow.getTasks().stream()
                .filter(t -> t.getReferenceTaskName().equals("task1_ref1"))
                .collect(groupingBy(Task::getReferenceTaskName, maxBy(comparingInt(Task::getSeq)))).values()
                .stream().map(Optional::get)
                .collect(Collectors.toList()).get(0);
        lastTask2.setStatus(Status.FAILED);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        workflowExecutor.retry(workflow.getWorkflowId());

        assertEquals(6, workflow.getTasks().size());
    }

    @Test
    public void testRetryWorkflowWithJoinTask() {
        //setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        workflow.setWorkflowType("testRetryWorkflowId");
        workflow.setVersion(1);
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
        task_1_1.setReferenceTaskName("task1_ref1");

        Task task_2_1 = new Task();
        task_2_1.setTaskId(UUID.randomUUID().toString());
        task_2_1.setSeq(22);
        task_2_1.setRetryCount(1);
        task_2_1.setStatus(Status.CANCELED);
        task_2_1.setTaskType(TaskType.SIMPLE.toString());
        task_2_1.setTaskDefName("task2");
        task_2_1.setReferenceTaskName("task2_ref1");

        Task joinTask = new Task();
        joinTask.setTaskType(TaskType.JOIN.toString());
        joinTask.setTaskId(UUID.randomUUID().toString());
        joinTask.setSeq(25);
        joinTask.setRetryCount(1);
        joinTask.setStatus(Status.CANCELED);
        joinTask.setReferenceTaskName("task_join");
        joinTask.getInputData().put("joinOn", Arrays.asList(task_1_1.getReferenceTaskName(), task_2_1.getReferenceTaskName()));

        workflow.getTasks().addAll(Arrays.asList(forkTask, task_1_1, task_2_1, joinTask));
        //end of setup

        //when:
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);
        WorkflowDef workflowDef = new WorkflowDef();
        when(metadataDAO.get(anyString(), anyInt())).thenReturn(Optional.of(workflowDef));

        workflowExecutor.retry(workflow.getWorkflowId());

        assertEquals(6, workflow.getTasks().size());
        assertEquals(Workflow.WorkflowStatus.RUNNING, workflow.getStatus());
    }

    @Test
    public void testGetActiveDomain() {
        String taskType = "test-task";
        String[] domains = new String[]{"domain1", "domain2"};

        PollData pollData1 = new PollData("queue1", domains[0], "worker1", System.currentTimeMillis() - 99 * 1000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[0])).thenReturn(pollData1);
        String activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(domains[0], activeDomain);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        PollData pollData2 = new PollData("queue2", domains[1], "worker2", System.currentTimeMillis() - 99 * 1000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[1])).thenReturn(pollData2);
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(domains[1], activeDomain);

        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(domains[1], activeDomain);

        domains = new String[]{""};
        when(executionDAOFacade.getTaskPollDataByDomain(any(), any())).thenReturn(new PollData());
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertNotNull(activeDomain);
        assertEquals("", activeDomain);

        domains = new String[]{};
        activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertNull(activeDomain);

        activeDomain = workflowExecutor.getActiveDomain(taskType, null);
        assertNull(activeDomain);
    }

    @Test
    public void testInactiveDomains() {
        String taskType = "test-task";
        String[] domains = new String[]{"domain1", "domain2"};

        PollData pollData1 = new PollData("queue1", domains[0], "worker1", System.currentTimeMillis() - 99 * 10000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[0])).thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[1])).thenReturn(null);
        String activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals("domain2", activeDomain);
    }

    @Test
    public void testDefaultDomain() {
        String taskType = "test-task";
        String[] domains = new String[]{"domain1", "domain2", "NO_DOMAIN"};

        PollData pollData1 = new PollData("queue1", domains[0], "worker1", System.currentTimeMillis() - 99 * 10000);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[0])).thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(taskType, domains[1])).thenReturn(null);
        String activeDomain = workflowExecutor.getActiveDomain(taskType, domains);
        assertEquals(null, activeDomain);
    }

    @Test
    public void testTaskToDomain() {
        Workflow workflow = generateSampleWorkflow();
        List<Task> tasks = generateSampleTasks(3);

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "mydomain");
        workflow.setTaskToDomain(taskToDomain);

        PollData pollData1 = new PollData("queue1", "mydomain", "worker1", System.currentTimeMillis() - 99 * 100);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), anyString())).thenReturn(pollData1);
        workflowExecutor.setTaskDomains(tasks, workflow);

        tasks.forEach(task -> assertEquals("mydomain", task.getDomain()));
    }

    @Test
    public void testTaskToDomainsPerTask() {
        Workflow workflow = generateSampleWorkflow();
        List<Task> tasks = generateSampleTasks(2);

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "mydomain, NO_DOMAIN");
        workflow.setTaskToDomain(taskToDomain);

        PollData pollData1 = new PollData("queue1", "mydomain", "worker1", System.currentTimeMillis() - 99 * 100);
        when(executionDAOFacade.getTaskPollDataByDomain(eq("task1"), anyString())).thenReturn(pollData1);
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

        PollData pollData1 = new PollData("queue1", "mydomain", "worker1", System.currentTimeMillis() - 99 * 100);
        PollData pollData2 = new PollData("queue2", "someActiveDomain", "worker2", System.currentTimeMillis() - 99 * 100);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("mydomain"))).thenReturn(pollData1);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("someInactiveDomain"))).thenReturn(null);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("someActiveDomain"))).thenReturn(pollData2);
        when(executionDAOFacade.getTaskPollDataByDomain(anyString(), eq("someInactiveDomain"))).thenReturn(null);
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

    @Test
    public void testRollbackTasks() {
        String workflowId = "workflow-id";

        Task task1 = new Task();
        task1.setTaskType(TaskType.SIMPLE.name());
        task1.setTaskDefName("simpleTask");
        task1.setReferenceTaskName("simpleTask");
        task1.setWorkflowInstanceId(workflowId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(IDGenerator.generate());
        task1.setStatus(Status.SCHEDULED);

        WorkflowTask waitTask = new WorkflowTask();
        waitTask.setWorkflowTaskType(TaskType.WAIT);
        waitTask.setType(TaskType.WAIT.name());
        waitTask.setTaskReferenceName("wait");
        Task task2 = new Task();
        task2.setTaskType(waitTask.getType());
        task2.setTaskDefName(waitTask.getName());
        task2.setReferenceTaskName(waitTask.getTaskReferenceName());
        task2.setWorkflowInstanceId(workflowId);
        task2.setScheduledTime(System.currentTimeMillis());
        task2.setTaskId(IDGenerator.generate());
        task2.setStatus(Status.IN_PROGRESS);
        task2.setRetryCount(0);
        task2.setWorkflowTask(waitTask);

        WorkflowTask subWorkflowTask = new WorkflowTask();
        subWorkflowTask.setWorkflowTaskType(TaskType.SUB_WORKFLOW);
        subWorkflowTask.setType(TaskType.SUB_WORKFLOW.name());
        subWorkflowTask.setTaskReferenceName("sub-workflow");
        Task task3 = new Task();
        task3.setTaskType(subWorkflowTask.getType());
        task3.setTaskDefName(subWorkflowTask.getName());
        task3.setReferenceTaskName(subWorkflowTask.getTaskReferenceName());
        task3.setWorkflowInstanceId(workflowId);
        task3.setScheduledTime(System.currentTimeMillis());
        task3.setTaskId(IDGenerator.generate());
        task3.setStatus(Status.IN_PROGRESS);
        task3.setRetryCount(0);
        task3.setWorkflowTask(subWorkflowTask);
        task3.setOutputData(new HashMap<>());
        task3.getOutputData().put(SUB_WORKFLOW_ID, IDGenerator.generate());

        AtomicInteger removeWorkflowCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            removeWorkflowCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).removeWorkflow(anyString(), anyBoolean());

        AtomicInteger removeTaskCalledCounter = new AtomicInteger(0);
        doAnswer(invocation -> {
            removeTaskCalledCounter.incrementAndGet();
            return null;
        }).when(executionDAOFacade).removeTask(anyString());

        workflowExecutor.rollbackTasks(workflowId, Arrays.asList(task1, task2, task3));
        assertEquals(1, removeWorkflowCalledCounter.get());
        assertEquals(3, removeTaskCalledCounter.get());
    }

    @Test(expected = ApplicationException.class)
    public void testTerminateWorkflow() {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testTerminateTerminalWorkflow");
        workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.terminateWorkflow(workflow.getWorkflowId(), "test terminating terminal workflow");
    }

    @Test
    public void testExecuteSystemTask() {
        String workflowId = "workflow-id";

        Wait wait = new Wait();

        String task1Id = IDGenerator.generate();
        Task task1 = new Task();
        task1.setTaskType(TaskType.WAIT.name());
        task1.setReferenceTaskName("waitTask");
        task1.setWorkflowInstanceId(workflowId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(task1Id);
        task1.setStatus(Status.SCHEDULED);

        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

        when(executionDAOFacade.getTaskById(anyString())).thenReturn(task1);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.executeSystemTask(wait, task1Id,30);

        assertEquals(Status.IN_PROGRESS, task1.getStatus());
    }

    @Test
    public void testExecuteSystemTaskWithAsyncComplete() {
        String workflowId = "workflow-id";

        Terminate terminate = new Terminate();

        String task1Id = IDGenerator.generate();
        Task task1 = new Task();
        task1.setTaskType(TaskType.WAIT.name());
        task1.setReferenceTaskName("waitTask");
        task1.setWorkflowInstanceId(workflowId);
        task1.setScheduledTime(System.currentTimeMillis());
        task1.setTaskId(task1Id);
        task1.getInputData().put("asyncComplete", true);
        task1.setStatus(Status.IN_PROGRESS);

        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

        when(executionDAOFacade.getTaskById(anyString())).thenReturn(task1);
        when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

        workflowExecutor.executeSystemTask(terminate, task1Id,30);

        // An asyncComplete task shouldn't be executed through this logic, and the Terminate task should remain IN_PROGRESS.
        assertEquals(Status.IN_PROGRESS, task1.getStatus());
    }

    private Workflow generateSampleWorkflow() {
        //setup
        Workflow workflow = new Workflow();
        workflow.setWorkflowId("testRetryWorkflowId");
        workflow.setWorkflowType("testRetryWorkflowId");
        workflow.setVersion(1);
        workflow.setOwnerApp("junit_testRetryWorkflowId");
        workflow.setStartTime(10L);
        workflow.setEndTime(100L);
        //noinspection unchecked
        workflow.setOutput(Collections.EMPTY_MAP);
        workflow.setStatus(Workflow.WorkflowStatus.FAILED);

        return workflow;
    }

    private List<Task> generateSampleTasks(int count) {
        if (count == 0) return null;
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Task task = new Task();
            task.setTaskId(UUID.randomUUID().toString());
            task.setSeq(i);
            task.setRetryCount(1);
            task.setTaskType("task" + (i+1));
            task.setStatus(Status.COMPLETED);
            task.setTaskDefName("taskX");
            task.setReferenceTaskName("task_ref" + (i+1));
            tasks.add(task);
        }

        return tasks;
    }

}

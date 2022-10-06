/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.core.reconciliation;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.*;
import com.netflix.conductor.core.operation.StartWorkflowOperation;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.metadata.tasks.TaskType.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestWorkflowRepairService {

    private QueueDAO queueDAO;
    private ExecutionDAO executionDAO;
    private ConductorProperties properties;
    private WorkflowRepairService workflowRepairService;
    private SystemTaskRegistry systemTaskRegistry;

    @Before
    public void setUp() {
        executionDAO = mock(ExecutionDAO.class);
        queueDAO = mock(QueueDAO.class);
        properties = mock(ConductorProperties.class);
        systemTaskRegistry = mock(SystemTaskRegistry.class);
        workflowRepairService =
                new WorkflowRepairService(executionDAO, queueDAO, properties, systemTaskRegistry);
    }

    @Test
    public void verifyAndRepairSimpleTaskInScheduledState() {
        TaskModel task = new TaskModel();
        task.setTaskType("SIMPLE");
        task.setStatus(TaskModel.Status.SCHEDULED);
        task.setTaskId("abcd");
        task.setCallbackAfterSeconds(60);

        when(queueDAO.containsMessage(anyString(), anyString())).thenReturn(false);

        assertTrue(workflowRepairService.verifyAndRepairTask(task));
        // Verify that a new queue message is pushed for sync system tasks that fails queue contains
        // check.
        verify(queueDAO, times(1)).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void verifySimpleTaskInProgressState() {
        TaskModel task = new TaskModel();
        task.setTaskType("SIMPLE");
        task.setStatus(TaskModel.Status.IN_PROGRESS);
        task.setTaskId("abcd");
        task.setCallbackAfterSeconds(60);

        when(queueDAO.containsMessage(anyString(), anyString())).thenReturn(false);

        assertFalse(workflowRepairService.verifyAndRepairTask(task));
        // Verify that queue message is never pushed for simple task in IN_PROGRESS state
        verify(queueDAO, never()).containsMessage(anyString(), anyString());
        verify(queueDAO, never()).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void verifyAndRepairSystemTask() {
        String taskType = "TEST_SYS_TASK";
        TaskModel task = new TaskModel();
        task.setTaskType(taskType);
        task.setStatus(TaskModel.Status.SCHEDULED);
        task.setTaskId("abcd");
        task.setCallbackAfterSeconds(60);

        when(systemTaskRegistry.isSystemTask("TEST_SYS_TASK")).thenReturn(true);
        when(systemTaskRegistry.get(taskType))
                .thenReturn(
                        new WorkflowSystemTask("TEST_SYS_TASK") {
                            @Override
                            public boolean isAsync() {
                                return true;
                            }

                            @Override
                            public boolean isAsyncComplete(TaskModel task) {
                                return false;
                            }

                            @Override
                            public void start(
                                    WorkflowModel workflow,
                                    TaskModel task,
                                    WorkflowExecutor executor) {
                                super.start(workflow, task, executor);
                            }
                        });

        when(queueDAO.containsMessage(anyString(), anyString())).thenReturn(false);

        assertTrue(workflowRepairService.verifyAndRepairTask(task));
        // Verify that a new queue message is pushed for tasks that fails queue contains check.
        verify(queueDAO, times(1)).push(anyString(), anyString(), anyLong());

        // Verify a system task in IN_PROGRESS state can be recovered.
        reset(queueDAO);
        task.setStatus(TaskModel.Status.IN_PROGRESS);
        assertTrue(workflowRepairService.verifyAndRepairTask(task));
        // Verify that a new queue message is pushed for async System task in IN_PROGRESS state that
        // fails queue contains check.
        verify(queueDAO, times(1)).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void assertSyncSystemTasksAreNotCheckedAgainstQueue() {
        // Return a Switch task object to init WorkflowSystemTask registry.
        when(systemTaskRegistry.get(TASK_TYPE_DECISION)).thenReturn(new Decision());
        when(systemTaskRegistry.isSystemTask(TASK_TYPE_DECISION)).thenReturn(true);
        when(systemTaskRegistry.get(TASK_TYPE_SWITCH)).thenReturn(new Switch());
        when(systemTaskRegistry.isSystemTask(TASK_TYPE_SWITCH)).thenReturn(true);

        TaskModel task = new TaskModel();
        task.setTaskType(TASK_TYPE_DECISION);
        task.setStatus(TaskModel.Status.SCHEDULED);

        assertFalse(workflowRepairService.verifyAndRepairTask(task));
        // Verify that queue contains is never checked for sync system tasks
        verify(queueDAO, never()).containsMessage(anyString(), anyString());
        // Verify that queue message is never pushed for sync system tasks
        verify(queueDAO, never()).push(anyString(), anyString(), anyLong());

        task = new TaskModel();
        task.setTaskType(TASK_TYPE_SWITCH);
        task.setStatus(TaskModel.Status.SCHEDULED);

        assertFalse(workflowRepairService.verifyAndRepairTask(task));
        // Verify that queue contains is never checked for sync system tasks
        verify(queueDAO, never()).containsMessage(anyString(), anyString());
        // Verify that queue message is never pushed for sync system tasks
        verify(queueDAO, never()).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void assertAsyncCompleteInProgressSystemTasksAreNotCheckedAgainstQueue() {
        TaskModel task = new TaskModel();
        task.setTaskType(TASK_TYPE_EVENT);
        task.setStatus(TaskModel.Status.IN_PROGRESS);
        task.setTaskId("abcd");
        task.setCallbackAfterSeconds(60);
        task.setInputData(Map.of("asyncComplete", true));

        WorkflowSystemTask workflowSystemTask =
                new Event(
                        mock(EventQueues.class),
                        mock(ParametersUtils.class),
                        mock(ObjectMapper.class));
        when(systemTaskRegistry.get(TASK_TYPE_EVENT)).thenReturn(workflowSystemTask);

        assertTrue(workflowSystemTask.isAsyncComplete(task));

        assertFalse(workflowRepairService.verifyAndRepairTask(task));
        // Verify that queue message is never pushed for async complete system tasks
        verify(queueDAO, never()).containsMessage(anyString(), anyString());
        verify(queueDAO, never()).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void assertAsyncCompleteScheduledSystemTasksAreCheckedAgainstQueue() {
        TaskModel task = new TaskModel();
        task.setTaskType(TASK_TYPE_SUB_WORKFLOW);
        task.setStatus(TaskModel.Status.SCHEDULED);
        task.setTaskId("abcd");
        task.setCallbackAfterSeconds(60);

        WorkflowSystemTask workflowSystemTask =
                new SubWorkflow(new ObjectMapper(), mock(StartWorkflowOperation.class));
        when(systemTaskRegistry.get(TASK_TYPE_SUB_WORKFLOW)).thenReturn(workflowSystemTask);
        when(queueDAO.containsMessage(anyString(), anyString())).thenReturn(false);

        assertTrue(workflowSystemTask.isAsyncComplete(task));

        assertTrue(workflowRepairService.verifyAndRepairTask(task));
        // Verify that queue message is never pushed for async complete system tasks
        verify(queueDAO, times(1)).containsMessage(anyString(), anyString());
        verify(queueDAO, times(1)).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void verifyAndRepairParentWorkflow() {
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId("abcd");
        workflow.setParentWorkflowId("parentWorkflowId");

        when(properties.getWorkflowOffsetTimeout()).thenReturn(Duration.ofSeconds(10));
        when(executionDAO.getWorkflow("abcd", true)).thenReturn(workflow);
        when(queueDAO.containsMessage(anyString(), anyString())).thenReturn(false);

        workflowRepairService.verifyAndRepairWorkflowTasks("abcd");
        verify(queueDAO, times(1)).containsMessage(anyString(), anyString());
        verify(queueDAO, times(1)).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void assertInProgressSubWorkflowSystemTasksAreCheckedAndRepaired() {
        String subWorkflowId = "subWorkflowId";
        String taskId = "taskId";

        TaskModel task = new TaskModel();
        task.setTaskType(TASK_TYPE_SUB_WORKFLOW);
        task.setStatus(TaskModel.Status.IN_PROGRESS);
        task.setTaskId(taskId);
        task.setCallbackAfterSeconds(60);
        task.setSubWorkflowId(subWorkflowId);
        Map<String, Object> outputMap = new HashMap<>();
        outputMap.put("subWorkflowId", subWorkflowId);
        task.setOutputData(outputMap);

        WorkflowModel subWorkflow = new WorkflowModel();
        subWorkflow.setWorkflowId(subWorkflowId);
        subWorkflow.setStatus(WorkflowModel.Status.TERMINATED);
        subWorkflow.setOutput(Map.of("k1", "v1", "k2", "v2"));

        when(executionDAO.getWorkflow(subWorkflowId, false)).thenReturn(subWorkflow);

        assertTrue(workflowRepairService.verifyAndRepairTask(task));
        // Verify that queue message is never pushed for async complete system tasks
        verify(queueDAO, never()).containsMessage(anyString(), anyString());
        verify(queueDAO, never()).push(anyString(), anyString(), anyLong());
        // Verify
        ArgumentCaptor<TaskModel> argumentCaptor = ArgumentCaptor.forClass(TaskModel.class);
        verify(executionDAO, times(1)).updateTask(argumentCaptor.capture());
        assertEquals(taskId, argumentCaptor.getValue().getTaskId());
        assertEquals(subWorkflowId, argumentCaptor.getValue().getSubWorkflowId());
        assertEquals(TaskModel.Status.CANCELED, argumentCaptor.getValue().getStatus());
        assertNotNull(argumentCaptor.getValue().getOutputData());
        assertEquals(subWorkflowId, argumentCaptor.getValue().getOutputData().get("subWorkflowId"));
        assertEquals("v1", argumentCaptor.getValue().getOutputData().get("k1"));
        assertEquals("v2", argumentCaptor.getValue().getOutputData().get("k2"));
    }
}

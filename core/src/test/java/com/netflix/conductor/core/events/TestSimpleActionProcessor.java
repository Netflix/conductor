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
package com.netflix.conductor.core.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.common.metadata.events.EventHandler.Action.Type;
import com.netflix.conductor.common.metadata.events.EventHandler.StartWorkflow;
import com.netflix.conductor.common.metadata.events.EventHandler.TaskDetails;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.tasks.TaskResult.Status;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSimpleActionProcessor {
    private WorkflowExecutor workflowExecutor;
    private SimpleActionProcessor actionProcessor;
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        workflowExecutor = mock(WorkflowExecutor.class);

        actionProcessor = new SimpleActionProcessor(workflowExecutor, new ParametersUtils(), new JsonUtils());
        objectMapper = new JsonMapperProvider().get();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStartWorkflow_correlationId() throws Exception {
        StartWorkflow startWorkflow = new StartWorkflow();
        startWorkflow.setName("testWorkflow");
        startWorkflow.getInput().put("testInput", "${testId}");
        startWorkflow.setCorrelationId("${correlationId}");

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "dev");
        startWorkflow.setTaskToDomain(taskToDomain);

        Action action = new Action();
        action.setAction(Type.start_workflow);
        action.setStart_workflow(startWorkflow);

        Object payload = objectMapper.readValue("{\"correlationId\":\"test-id\", \"testId\":\"test_1\"}", Object.class);

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testWorkflow");
        workflowDef.setVersion(1);

        when(workflowExecutor.startWorkflow(eq("testWorkflow"), eq(null), any(), any(), any(), eq("testEvent"), anyMap()))
                .thenReturn("workflow_1");

        Map<String, Object> output = actionProcessor.execute(action, payload, "testEvent", "testMessage");

        assertNotNull(output);
        assertEquals("workflow_1", output.get("workflowId"));

        ArgumentCaptor<String> correlationIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> inputParamCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map> taskToDomainCaptor = ArgumentCaptor.forClass(Map.class);
        verify(workflowExecutor).startWorkflow(eq("testWorkflow"), eq(null), correlationIdCaptor.capture(), inputParamCaptor.capture(), any(), eq("testEvent"), taskToDomainCaptor.capture());
        assertEquals("test_1", inputParamCaptor.getValue().get("testInput"));
        assertEquals("test-id", correlationIdCaptor.getValue());
        assertEquals("testMessage", inputParamCaptor.getValue().get("conductor.event.messageId"));
        assertEquals("testEvent", inputParamCaptor.getValue().get("conductor.event.name"));
        assertEquals(taskToDomain, taskToDomainCaptor.getValue());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStartWorkflow() throws Exception {
        StartWorkflow startWorkflow = new StartWorkflow();
        startWorkflow.setName("testWorkflow");
        startWorkflow.getInput().put("testInput", "${testId}");

        Map<String, String> taskToDomain = new HashMap<>();
        taskToDomain.put("*", "dev");
        startWorkflow.setTaskToDomain(taskToDomain);

        Action action = new Action();
        action.setAction(Type.start_workflow);
        action.setStart_workflow(startWorkflow);

        Object payload = objectMapper.readValue("{\"testId\":\"test_1\"}", Object.class);

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName("testWorkflow");
        workflowDef.setVersion(1);

        when(workflowExecutor.startWorkflow(eq("testWorkflow"), eq(null), any(), any(), any(), eq("testEvent"), anyMap()))
                .thenReturn("workflow_1");

        Map<String, Object> output = actionProcessor.execute(action, payload, "testEvent", "testMessage");

        assertNotNull(output);
        assertEquals("workflow_1", output.get("workflowId"));

        ArgumentCaptor<String> correlationIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> inputParamCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map> taskToDomainCaptor = ArgumentCaptor.forClass(Map.class);
        verify(workflowExecutor).startWorkflow(eq("testWorkflow"), eq(null), correlationIdCaptor.capture(), inputParamCaptor.capture(), any(), eq("testEvent"), taskToDomainCaptor.capture());
        assertEquals("test_1", inputParamCaptor.getValue().get("testInput"));
        assertNull(correlationIdCaptor.getValue());
        assertEquals("testMessage", inputParamCaptor.getValue().get("conductor.event.messageId"));
        assertEquals("testEvent", inputParamCaptor.getValue().get("conductor.event.name"));
        assertEquals(taskToDomain, taskToDomainCaptor.getValue());
    }

    @Test
    public void testCompleteTask() throws Exception {
        TaskDetails taskDetails = new TaskDetails();
        taskDetails.setWorkflowId("${workflowId}");
        taskDetails.setTaskRefName("testTask");
        taskDetails.getOutput().put("someNEKey", "${Message.someNEKey}");
        taskDetails.getOutput().put("someKey", "${Message.someKey}");
        taskDetails.getOutput().put("someNullKey", "${Message.someNullKey}");

        Action action = new Action();
        action.setAction(Type.complete_task);
        action.setComplete_task(taskDetails);

        String payloadJson = "{\"workflowId\":\"workflow_1\",\"Message\":{\"someKey\":\"someData\",\"someNullKey\":null}}";
        Object payload = objectMapper.readValue(payloadJson, Object.class);

        Task task = new Task();
        task.setReferenceTaskName("testTask");
        Workflow workflow = new Workflow();
        workflow.getTasks().add(task);

        when(workflowExecutor.getWorkflow(eq("workflow_1"), anyBoolean())).thenReturn(workflow);

        actionProcessor.execute(action, payload, "testEvent", "testMessage");

        ArgumentCaptor<TaskResult> argumentCaptor = ArgumentCaptor.forClass(TaskResult.class);
        verify(workflowExecutor).updateTask(argumentCaptor.capture());
        assertEquals(Status.COMPLETED, argumentCaptor.getValue().getStatus());
        assertEquals("testMessage", argumentCaptor.getValue().getOutputData().get("conductor.event.messageId"));
        assertEquals("testEvent", argumentCaptor.getValue().getOutputData().get("conductor.event.name"));
        assertEquals("workflow_1", argumentCaptor.getValue().getOutputData().get("workflowId"));
        assertEquals("testTask", argumentCaptor.getValue().getOutputData().get("taskRefName"));
        assertEquals("someData", argumentCaptor.getValue().getOutputData().get("someKey"));
        // Assert values not in message are evaluated to null
        assertTrue("testTask", argumentCaptor.getValue().getOutputData().containsKey("someNEKey"));
        // Assert null values from message are kept
        assertTrue("testTask", argumentCaptor.getValue().getOutputData().containsKey("someNullKey"));
        assertNull("testTask", argumentCaptor.getValue().getOutputData().get("someNullKey"));
    }

    @Test
    public void testCompleteTaskByTaskId() throws Exception {
        TaskDetails taskDetails = new TaskDetails();
        taskDetails.setWorkflowId("${workflowId}");
        taskDetails.setTaskId("${taskId}");

        Action action = new Action();
        action.setAction(Type.complete_task);
        action.setComplete_task(taskDetails);

        Object payload = objectMapper.readValue("{\"workflowId\":\"workflow_1\", \"taskId\":\"task_1\"}", Object.class);

        Task task = new Task();
        task.setTaskId("task_1");
        task.setReferenceTaskName("testTask");

        when(workflowExecutor.getTask(eq("task_1"))).thenReturn(task);

        actionProcessor.execute(action, payload, "testEvent", "testMessage");

        ArgumentCaptor<TaskResult> argumentCaptor = ArgumentCaptor.forClass(TaskResult.class);
        verify(workflowExecutor).updateTask(argumentCaptor.capture());
        assertEquals(Status.COMPLETED, argumentCaptor.getValue().getStatus());
        assertEquals("testMessage", argumentCaptor.getValue().getOutputData().get("conductor.event.messageId"));
        assertEquals("testEvent", argumentCaptor.getValue().getOutputData().get("conductor.event.name"));
        assertEquals("workflow_1", argumentCaptor.getValue().getOutputData().get("workflowId"));
        assertEquals("task_1", argumentCaptor.getValue().getOutputData().get("taskId"));
    }
}

/**
 * Copyright 2021 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.testing.workflows;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.sdk.executor.WorkflowExecutor;
import com.netflix.conductor.sdk.task.IpParam;
import com.netflix.conductor.sdk.task.OpParam;
import com.netflix.conductor.sdk.task.WorkflowTask;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class KitchenSinkTest {

    private static WorkflowExecutor executor;

    @BeforeClass
    public static void init() throws IOException {
        executor = WorkflowExecutor.getInstance();
        executor.startServerAndPolling("com.netflix.conductor");
        executor.loadTaskDefs("/tasks.json");
        executor.loadWorkflowDefs("/simple_workflow.json");
    }

    @AfterClass
    public static void cleanUp() {
        WorkflowExecutor.getInstance().shutdown();
    }

    @Test
    public void testDynamicTaskExecuted() {
        Map<String, Object> input = new HashMap<>();
        input.put("task2Name", "task_2");
        input.put("mod", "1");
        input.put("oddEven", "12");
        input.put("number", 0);

        //Start the workflow and wait for it to complete
        Workflow workflow = executor.executeWorkflow("Decision_TaskExample", 1, input);

        assertNotNull(workflow);
        assertEquals(Workflow.WorkflowStatus.COMPLETED, workflow.getStatus());
        assertNotNull(workflow.getOutput());
        assertTrue(workflow.getTasks().stream().anyMatch(task -> task.getTaskDefName().equals("task_6")));

        //task_2's implementation fails at the first try, so we should have to instances of task_2 execution
        //2 executions of task_2 should be present
        assertEquals(2, workflow.getTasks().stream().filter(task -> task.getTaskDefName().equals("task_2")).count());
        List<Task> task2Executions = workflow.getTasks()
                .stream().filter(task -> task.getTaskDefName().equals("task_2")).collect(Collectors.toList());
        assertNotNull(task2Executions);
        assertEquals(2, task2Executions.size());

        //First instance would have failed and second succeeded.
        assertEquals(Task.Status.FAILED, task2Executions.get(0).getStatus());
        assertEquals(Task.Status.COMPLETED, task2Executions.get(1).getStatus());

        //task10's output
        assertEquals(100, workflow.getOutput().get("c"));
    }

    @Test
    public void testWorkflowFailure() {
        Map<String, Object> input = new HashMap<>();
        //task2Name is missing which will cause workflow to fail
        input.put("mod", "1");
        input.put("oddEven", "12");
        input.put("number", 0);

        //we are missing task2Name parameter which is required to wire up dynamictask
        //The workflow should fail as we are not passing it as input
        Workflow workflow = executor.executeWorkflow("Decision_TaskExample", 1, input);
        assertNotNull(workflow);
        assertEquals(Workflow.WorkflowStatus.FAILED, workflow.getStatus());
        assertNotNull(workflow.getReasonForIncompletion());
    }

    @WorkflowTask("task_1")
    public Map<String, Object> task1(Task1Input input) {
        Map<String, Object> result = new HashMap<>();
        result.put("input", input);
        return result;
    }

    @WorkflowTask("task_2")
    public TaskResult task2(Task task) {
        if(task.getRetryCount() < 1) {
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion("try again");
            return new TaskResult(task);
        }

        task.setStatus(Task.Status.COMPLETED);
        return new TaskResult(task);
    }

    @WorkflowTask("task_6")
    public TaskResult task6(Task task) {
        task.setStatus(Task.Status.COMPLETED);
        return new TaskResult(task);
    }

    @WorkflowTask("task_10")
    public TaskResult task10(Task task) {
        task.setStatus(Task.Status.COMPLETED);
        task.getOutputData().put("a", "b");
        task.getOutputData().put("c", 100);
        task.getOutputData().put("x", false);
        return new TaskResult(task);
    }

    @WorkflowTask("task_8")
    public TaskResult task8(Task task) {
        task.setStatus(Task.Status.COMPLETED);
        return new TaskResult(task);
    }

    @WorkflowTask("task_5")
    public TaskResult task5(Task task) {
        task.setStatus(Task.Status.COMPLETED);
        return new TaskResult(task);
    }

    @WorkflowTask("task_3")
    public @OpParam("z1") String task3(@IpParam("taskToExecute") String p1) {
        return "output of task3, p1=" + p1;
    }

    @WorkflowTask("task_30")
    public Map<String, Object> task30(Task task) {
        Map<String, Object> output = new HashMap<>();
        output.put("v1", "b");
        output.put("v2", Arrays.asList("one","two", 3));
        output.put("v3", 5);
        return output;
    }

    @WorkflowTask("task_31")
    public Map<String, Object> task31(Task task) {
        Map<String, Object> output = new HashMap<>();
        output.put("a1", "b");
        output.put("a2", Arrays.asList("one","two", 3));
        output.put("a3", 5);
        return output;
    }

    @WorkflowTask("HTTP")
    public Map<String, Object> http(Task task) {
        Map<String, Object> output = new HashMap<>();
        output.put("a1", "b");
        output.put("a2", Arrays.asList("one","two", 3));
        output.put("a3", 5);
        return output;
    }

    @WorkflowTask("EVENT")
    public Map<String, Object> event(Task task) {
        Map<String, Object> output = new HashMap<>();
        output.put("a1", "b");
        output.put("a2", Arrays.asList("one","two", 3));
        output.put("a3", 5);
        return output;
    }

}

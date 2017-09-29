package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Oleksiy Lysak
 *
 */
public class TestUpdateTask {
    private WorkflowExecutor executor = mock(WorkflowExecutor.class);
    private Workflow workflow = new Workflow();

    @Test
    public void no_status_param() throws Exception {
        UpdateTask updateTask = new UpdateTask();

        Task task = new Task();
        updateTask.start(workflow, task, executor);
        assertEquals(Task.Status.FAILED, task.getStatus());
        assertEquals("Missing 'status' in input parameters", task.getReasonForIncompletion());
    }

    @Test
    public void invalid_status_param() throws Exception {
        UpdateTask updateTask = new UpdateTask();

        Task task = new Task();
        task.getInputData().put("status", "SKIPPED");
        updateTask.start(workflow, task, executor);
        assertEquals(Task.Status.FAILED, task.getStatus());
        assertEquals("Invalid 'status' value. Allowed COMPLETED/COMPLETED_WITH_ERRORS/FAILED/IN_PROGRESS only", task.getReasonForIncompletion());
    }

    @Test
    public void no_workflowId_param() throws Exception {
        UpdateTask updateTask = new UpdateTask();

        Task task = new Task();
        task.getInputData().put("status", "COMPLETED");
        updateTask.start(workflow, task, executor);
        assertEquals(Task.Status.FAILED, task.getStatus());
        assertEquals("Missing 'workflowId' in input parameters", task.getReasonForIncompletion());
    }

    @Test
    public void no_taskRefName_param() throws Exception {
        UpdateTask updateTask = new UpdateTask();

        Task task = new Task();
        task.getInputData().put("status", "COMPLETED");
        task.getInputData().put("workflowId", "1");
        updateTask.start(workflow, task, executor);
        assertEquals(Task.Status.FAILED, task.getStatus());
        assertEquals("Missing 'taskRefName' in input parameters", task.getReasonForIncompletion());
    }

    @Test
    public void no_workflow_found() throws Exception {
        UpdateTask updateTask = new UpdateTask();

        Task task = new Task();
        task.getInputData().put("status", "COMPLETED");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");
        updateTask.start(workflow, task, executor);

        assertNotNull(task.getOutputData());
        assertEquals(Task.Status.COMPLETED_WITH_ERRORS, task.getStatus());
        assertEquals("No workflow found with id 1", task.getOutputData().get("error"));
    }

    @Test
    public void no_task_found() throws Exception {
        Workflow workflow = mock(Workflow.class);
        when(workflow.getTaskByRefName("wait")).thenReturn(null);

        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        when(executor.getWorkflow("1", true)).thenReturn(workflow);

        UpdateTask updateTask = new UpdateTask();

        Task task = new Task();
        task.getInputData().put("status", "COMPLETED");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");
        updateTask.start(workflow, task, executor);

        assertNotNull(task.getOutputData());
        assertEquals(Task.Status.COMPLETED_WITH_ERRORS, task.getStatus());
        assertEquals("No task found with reference name wait, workflowId 1", task.getOutputData().get("error"));
    }

    @Test
    public void update_task_fails() throws Exception {
        Task task = new Task();
        task.getInputData().put("status", "COMPLETED");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");

        Workflow workflow = mock(Workflow.class);
        when(workflow.getTaskByRefName("wait")).thenReturn(task);

        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        when(executor.getWorkflow("1", true)).thenReturn(workflow);
        doThrow(new RuntimeException("test")).when(executor).updateTask(any(TaskResult.class));

        UpdateTask updateTask = new UpdateTask();
        updateTask.start(workflow, task, executor);

        assertNotNull(task.getOutputData());
        assertEquals(Task.Status.COMPLETED_WITH_ERRORS, task.getStatus());
        assertEquals("Unable to update task: test", task.getOutputData().get("error"));
    }

    @Test
    public void completed() throws Exception {
        Task task = new Task();
        task.getInputData().put("status", "COMPLETED");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");

        Workflow workflow = mock(Workflow.class);
        when(workflow.getTaskByRefName("wait")).thenReturn(task);

        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        when(executor.getWorkflow("1", true)).thenReturn(workflow);
        doNothing().when(executor).updateTask(any(TaskResult.class));

        UpdateTask updateTask = new UpdateTask();
        updateTask.start(workflow, task, executor);

        assertEquals(Task.Status.COMPLETED, task.getStatus());
        assertNotNull(task.getOutputData());
    }

    @Test
    public void completed_with_errors() throws Exception {
        Task task = new Task();
        task.getInputData().put("status", "COMPLETED_WITH_ERRORS");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");

        Workflow workflow = mock(Workflow.class);
        when(workflow.getTaskByRefName("wait")).thenReturn(task);

        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        when(executor.getWorkflow("1", true)).thenReturn(workflow);
        doNothing().when(executor).updateTask(any(TaskResult.class));

        UpdateTask updateTask = new UpdateTask();
        updateTask.start(workflow, task, executor);

        assertEquals(Task.Status.COMPLETED_WITH_ERRORS, task.getStatus());
        assertNotNull(task.getOutputData());
    }

    @Test
    public void failed() throws Exception {
        Task task = new Task();
        task.getInputData().put("status", "FAILED");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");

        Workflow workflow = mock(Workflow.class);
        when(workflow.getTaskByRefName("wait")).thenReturn(task);

        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        when(executor.getWorkflow("1", true)).thenReturn(workflow);
        doNothing().when(executor).updateTask(any(TaskResult.class));

        UpdateTask updateTask = new UpdateTask();
        updateTask.start(workflow, task, executor);

        assertEquals(Task.Status.FAILED, task.getStatus());
        assertNotNull(task.getOutputData());
    }

    @Test
    public void in_progress() throws Exception {
        Task task = new Task();
        task.getInputData().put("status", "IN_PROGRESS");
        task.getInputData().put("workflowId", "1");
        task.getInputData().put("taskRefName", "wait");

        Workflow workflow = mock(Workflow.class);
        when(workflow.getTaskByRefName("wait")).thenReturn(task);

        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        when(executor.getWorkflow("1", true)).thenReturn(workflow);
        doNothing().when(executor).updateTask(any(TaskResult.class));

        UpdateTask updateTask = new UpdateTask();
        updateTask.start(workflow, task, executor);

        assertEquals(Task.Status.IN_PROGRESS, task.getStatus());
        assertNotNull(task.getOutputData());
    }
}

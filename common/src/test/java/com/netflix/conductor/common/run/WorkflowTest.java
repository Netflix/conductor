package com.netflix.conductor.common.run;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class WorkflowTest {

    @Test
    public void testForFailedTaskIdWhenAllTasksHaveFailed() {
        Workflow workflow = new Workflow();
        List<Task> allFailedTasks = Arrays.asList(createTask("Foo", Task.Status.FAILED), createTask("Bar", Task.Status.FAILED));
        workflow.setTasks(allFailedTasks);
        assertEquals(workflow.getFailedTaskId(),"Foo");
    }

    @Test
    public void testForFailedTaskIdWhenOneTaskHasFailed() {
        Workflow workflow = new Workflow();
        List<Task> allFailedTasks = Arrays.asList(createTask("Foo", Task.Status.IN_PROGRESS), createTask("Bar", Task.Status.FAILED));
        workflow.setTasks(allFailedTasks);
        assertEquals(workflow.getFailedTaskId(),"Bar");
    }

    @Test
    public void testForFailedTaskIdWhenNoTasksHaveFailed() {
        Workflow workflow = new Workflow();
        List<Task> allFailedTasks = Arrays.asList(createTask("Foo", Task.Status.IN_PROGRESS), createTask("Bar", Task.Status.COMPLETED));
        workflow.setTasks(allFailedTasks);
        assertNull(workflow.getFailedTaskId());
    }

    private Task createTask(String taskName, Task.Status taskStatus) {
        Task task1 = new Task();
        task1.setTaskId(taskName);
        task1.setStatus(taskStatus);
        return task1;
    }

}
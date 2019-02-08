package com.netflix.conductor.common.run;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class TaskSummaryTest {

    @Test
    public void testJsonSerializing() throws Exception {
        ObjectMapper om = new ObjectMapper();

        Task task = new Task();
        TaskSummary taskSummary = new TaskSummary(task);

        String json = om.writeValueAsString(taskSummary);
        TaskSummary read = om.readValue(json, TaskSummary.class);
        assertNotNull(read);
    }

    @Test
    public void testTaskSummaryConstructorWithLoggingDisabled(){
        Task task = Mockito.mock(Task.class);
        when(task.isLoggingDisabled()).thenReturn(true);
        TaskSummary taskSummary = new TaskSummary(task);
        assertNull(taskSummary.getInput());
        assertNull(taskSummary.getOutput());
    }

    @Test
    public void testTaskSummaryConstructorWithLoggingEnabled(){
        Task task = Mockito.mock(Task.class);
        when(task.isLoggingDisabled()).thenReturn(false);
        TaskSummary taskSummary = new TaskSummary(task);
        assertNotNull(taskSummary.getInput());
        assertNotNull(taskSummary.getOutput());
    }

}

package com.netflix.conductor.core.execution.mapper;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.utils.IDGenerator;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class EventTaskMapperTest {



    @Test
    public void getMappedTasks() throws Exception {
        ParametersUtils parametersUtils = Mockito.mock(ParametersUtils.class);
        EventTaskMapper  eventTaskMapper = new EventTaskMapper(parametersUtils);

        WorkflowTask taskToBeScheduled = new WorkflowTask();
        taskToBeScheduled.setSink("SQSSINK");
        String taskId = IDGenerator.generate();

        Map<String, Object> eventTaskInput = new HashMap<>();
        eventTaskInput.put("sink","SQSSINK");

        when(parametersUtils.getTaskInput(anyMap(), any(Workflow.class), any(TaskDef.class), anyString())).thenReturn(eventTaskInput);

        TaskMapperContext taskMapperContext = new TaskMapperContext(new WorkflowDef(), new Workflow(), taskToBeScheduled, null, 0, null, taskId, null);

        List<Task> mappedTasks = eventTaskMapper.getMappedTasks(taskMapperContext);
        assertEquals(1, mappedTasks.size());

        Task eventTask = mappedTasks.get(0);
        assertEquals(taskId, eventTask.getTaskId());

    }

}
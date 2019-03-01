/*
 * Copyright 2016 Netflix, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class EventWaitTaskMapperTest {

    @Test
    public void getMappedTasks() throws Exception {
        ParametersUtils parametersUtils = Mockito.mock(ParametersUtils.class);
        EventWaitTaskMapper  eventWaitTaskMapper = new EventWaitTaskMapper(parametersUtils);

        WorkflowTask taskToBeScheduled = new WorkflowTask();
        taskToBeScheduled.setSink("SQSSINK");
        String taskId = IDGenerator.generate();

        Map<String, Object> eventWaitTaskInput = new HashMap<>();
        eventWaitTaskInput.put("sink","SQSSINK");

        when(parametersUtils.getTaskInput(anyMap(), any(Workflow.class), any(TaskDef.class), anyString())).thenReturn(eventWaitTaskInput);

        WorkflowDef wd = new WorkflowDef();
        Workflow w = new Workflow();
        w.setWorkflowDefinition(wd);

        TaskMapperContext taskMapperContext = TaskMapperContext.newBuilder()
                .withWorkflowDefinition(wd)
                .withWorkflowInstance(w)
                .withTaskDefinition(new TaskDef())
                .withTaskToSchedule(taskToBeScheduled)
                .withRetryCount(0)
                .withTaskId(taskId)
                .build();

        List<Task> mappedTasks = eventWaitTaskMapper.getMappedTasks(taskMapperContext);
        assertEquals(1, mappedTasks.size());

        Task eventWaitTask = mappedTasks.get(0);
        assertEquals(taskId, eventWaitTask.getTaskId());

    }
}
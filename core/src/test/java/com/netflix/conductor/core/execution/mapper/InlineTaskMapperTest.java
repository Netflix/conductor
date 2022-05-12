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
package com.netflix.conductor.core.execution.mapper;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.execution.evaluators.JavascriptEvaluator;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class InlineTaskMapperTest {

    private ParametersUtils parametersUtils;
    private MetadataDAO metadataDAO;

    @Before
    public void setUp() {
        parametersUtils = mock(ParametersUtils.class);
        metadataDAO = mock(MetadataDAO.class);
    }

    @Test
    public void getMappedTasks() {

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("inline_task");
        workflowTask.setType(TaskType.INLINE.name());
        workflowTask.setTaskDefinition(new TaskDef("inline_task"));
        workflowTask.setEvaluatorType(JavascriptEvaluator.NAME);
        workflowTask.setExpression(
                "function scriptFun() {if ($.input.a==1){return {testValue: true}} else{return "
                        + "{testValue: false} }}; scriptFun();");

        String taskId = new IDGenerator().generate();

        WorkflowDef workflowDef = new WorkflowDef();
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(new TaskDef())
                        .withWorkflowTask(workflowTask)
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        List<TaskModel> mappedTasks =
                new InlineTaskMapper(parametersUtils, metadataDAO)
                        .getMappedTasks(taskMapperContext);

        assertEquals(1, mappedTasks.size());
        assertNotNull(mappedTasks);
        assertEquals(TaskType.INLINE.name(), mappedTasks.get(0).getTaskType());
    }

    @Test
    public void getMappedTasks_WithoutTaskDef() {

        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setType(TaskType.INLINE.name());
        workflowTask.setEvaluatorType(JavascriptEvaluator.NAME);
        workflowTask.setExpression(
                "function scriptFun() {if ($.input.a==1){return {testValue: true}} else{return "
                        + "{testValue: false} }}; scriptFun();");

        String taskId = new IDGenerator().generate();

        WorkflowDef workflowDef = new WorkflowDef();
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowDefinition(workflowDef);

        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(null)
                        .withWorkflowTask(workflowTask)
                        .withRetryCount(0)
                        .withTaskId(taskId)
                        .build();

        List<TaskModel> mappedTasks =
                new InlineTaskMapper(parametersUtils, metadataDAO)
                        .getMappedTasks(taskMapperContext);

        assertEquals(1, mappedTasks.size());
        assertNotNull(mappedTasks);
        assertEquals(TaskType.INLINE.name(), mappedTasks.get(0).getTaskType());
    }
}

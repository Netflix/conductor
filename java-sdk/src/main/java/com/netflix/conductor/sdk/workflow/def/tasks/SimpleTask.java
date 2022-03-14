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
package com.netflix.conductor.sdk.workflow.def.tasks;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.sdk.workflow.utils.ObjectMapperProvider;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Workflow task executed by a worker */
public class SimpleTask extends Task<SimpleTask> {

    private static final int ONE_HOUR = 60 * 60;

    private final ObjectMapper objectMapper = new ObjectMapperProvider().getObjectMapper();

    private boolean useGlobalTaskDef;

    private TaskDef taskDef;

    public SimpleTask(String taskDefName, String taskReferenceName) {
        super(taskReferenceName, TaskType.SIMPLE);
        super.name(taskDefName);
        this.useGlobalTaskDef = false;
    }

    SimpleTask(WorkflowTask workflowTask) {
        super(workflowTask);
        if (workflowTask.getTaskDefinition() == null) {
            this.useGlobalTaskDef = true;
        } else {
            this.taskDef = workflowTask.getTaskDefinition();
        }
    }

    /**
     * When set workflow will use the task definition registered in conductor. Workflow registration
     * will fail if no task definitions are found in conductor server
     *
     * @return current instance
     */
    public SimpleTask useGlobalTaskDef() {
        this.useGlobalTaskDef = true;
        return this;
    }

    public TaskDef getTaskDef() {
        return taskDef;
    }

    public SimpleTask setTaskDef(TaskDef taskDef) {
        this.useGlobalTaskDef = false;
        this.taskDef = taskDef;
        return this;
    }

    @Override
    protected void updateWorkflowTask(WorkflowTask workflowTask) {
        if (this.taskDef != null) {
            workflowTask.setTaskDefinition(taskDef);
        }
        if (useGlobalTaskDef) {
            workflowTask.setTaskDefinition(null);
        }
    }
}

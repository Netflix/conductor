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

import java.util.ArrayList;
import java.util.List;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

public class DynamicFork extends Task<DynamicFork> {

    private String forkTasksParameter;

    private String forkTasksInputsParameter;

    private Join join;

    public DynamicFork(
            String taskReferenceName, String forkTasksParameter, String forkTasksInputsParameter) {
        super(taskReferenceName, TaskType.FORK_JOIN_DYNAMIC);
        this.join = new Join(taskReferenceName + "_join");
        this.forkTasksParameter = forkTasksParameter;
        this.forkTasksInputsParameter = forkTasksInputsParameter;
        super.input("forkedTasks", forkTasksParameter);
        super.input("forkedTasksInputs", forkTasksInputsParameter);
    }

    DynamicFork(WorkflowTask workflowTask) {
        super(workflowTask);
        String nameOfParamForForkTask = workflowTask.getDynamicForkTasksParam();
        String nameOfParamForForkTaskInput = workflowTask.getDynamicForkTasksInputParamName();
        this.forkTasksParameter =
                (String) workflowTask.getInputParameters().get(nameOfParamForForkTask);
        this.forkTasksInputsParameter =
                (String) workflowTask.getInputParameters().get(nameOfParamForForkTaskInput);
    }

    public Join getJoin() {
        return join;
    }

    public String getForkTasksParameter() {
        return forkTasksParameter;
    }

    public String getForkTasksInputsParameter() {
        return forkTasksInputsParameter;
    }

    @Override
    public void updateWorkflowTask(WorkflowTask task) {
        task.setDynamicForkTasksParam("forkedTasks");
        task.setDynamicForkTasksInputParamName("forkedTasksInputs");
    }

    @Override
    protected List<WorkflowTask> getChildrenTasks() {
        List<WorkflowTask> tasks = new ArrayList<>();
        tasks.addAll(join.getWorkflowDefTasks());
        return tasks;
    }
}

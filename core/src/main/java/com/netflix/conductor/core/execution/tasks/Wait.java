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
package com.netflix.conductor.core.execution.tasks;

import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.domain.TaskDO;
import com.netflix.conductor.domain.TaskStatusDO;
import com.netflix.conductor.domain.WorkflowDO;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;

@Component(TASK_TYPE_WAIT)
public class Wait extends WorkflowSystemTask {

    public Wait() {
        super(TASK_TYPE_WAIT);
    }

    @Override
    public void start(WorkflowDO workflow, TaskDO task, WorkflowExecutor workflowExecutor) {
        task.setStatus(TaskStatusDO.IN_PROGRESS);
    }

    @Override
    public boolean execute(WorkflowDO workflow, TaskDO task, WorkflowExecutor workflowExecutor) {
        return false;
    }

    @Override
    public void cancel(WorkflowDO workflow, TaskDO task, WorkflowExecutor workflowExecutor) {
        task.setStatus(TaskStatusDO.CANCELED);
    }
}

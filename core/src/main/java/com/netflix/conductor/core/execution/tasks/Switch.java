/*
 * Copyright 2021 Netflix, Inc.
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

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SWITCH;

/** {@link Switch} task is a replacement for now deprecated {@link Decision} task. */
@Component(TASK_TYPE_SWITCH)
public class Switch extends WorkflowSystemTask {

    public Switch() {
        super(TASK_TYPE_SWITCH);
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor workflowExecutor) {
        task.setStatus(Status.COMPLETED);
        return true;
    }
}

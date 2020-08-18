/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.Map;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetVariable extends WorkflowSystemTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(SetVariable.class);

    public static final String NAME = "SET_VARIABLE";

    public SetVariable() {
        super(NAME);
        LOGGER.info(NAME + " task initialized...");
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) {
        Map<String, Object> workflowVars = workflow.getVariables();
        Map<String, Object> input = task.getInputData();
        String taskId = task.getTaskId();

        if (input != null && input.size() > 0) {
            input.keySet().forEach(key -> {
                workflowVars.put(key, input.get(key));
                LOGGER.debug("Task: {} setting value for variable: {}", taskId, key);
            });
        }
        task.setStatus(Task.Status.COMPLETED);
        return true;
    }
}

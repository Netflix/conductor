/**
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
/**
 *
 */
package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.QueueUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Oleksiy Lysak
 *
 */
public class Batch extends WorkflowSystemTask {

    public static final String NAME = "BATCH";

    public Batch() {
        super(NAME);
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        try {
            String queueName = getQueueName(task);

            executor.getQueueDao().pushIfNotExists(queueName, task.getTaskId(), task.getCallbackAfterSeconds(), workflow.getJobPriority());
        } catch (Exception ex) {
            task.setStatus(Status.FAILED);
            task.setReasonForIncompletion(ex.getMessage());
        }
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        return false;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        String queueName = getQueueName(task);
        executor.getQueueDao().remove(queueName, task.getTaskId());
        task.setStatus(Status.CANCELED);
    }

    private String getQueueName(Task task) {
        String name = (String) task.getInputData().get("name");
        if (StringUtils.isEmpty(name)) {
            throw new RuntimeException("Batch 'name' input parameter required");
        }

        return QueueUtils.getQueueName(NAME + "." + name, null);
    }
}

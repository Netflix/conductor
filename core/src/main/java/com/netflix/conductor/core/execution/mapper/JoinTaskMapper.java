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
package com.netflix.conductor.core.execution.mapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;

/**
 * An implementation of {@link TaskMapper} to map a {@link WorkflowTask} of type {@link
 * TaskType#JOIN} to a {@link Task} of type {@link TaskType#JOIN}
 */
@Component
public class JoinTaskMapper implements TaskMapper {

    public static final Logger LOGGER = LoggerFactory.getLogger(JoinTaskMapper.class);

    @Override
    public TaskType getTaskType() {
        return TaskType.JOIN;
    }

    /**
     * This method maps {@link TaskMapper} to map a {@link WorkflowTask} of type {@link
     * TaskType#JOIN} to a {@link Task} of type {@link TaskType#JOIN} with a status of {@link
     * Task.Status#IN_PROGRESS}
     *
     * @param taskMapperContext: A wrapper class containing the {@link WorkflowTask}, {@link
     *     WorkflowDef}, {@link Workflow} and a string representation of the TaskId
     * @return A {@link Task} of type {@link TaskType#JOIN} in a List
     */
    @Override
    public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) {

        LOGGER.debug("TaskMapperContext {} in JoinTaskMapper", taskMapperContext);

        WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
        Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
        String taskId = taskMapperContext.getTaskId();

        Map<String, Object> joinInput = new HashMap<>();
        joinInput.put("joinOn", taskToSchedule.getJoinOn());

        Task joinTask = new Task();
        joinTask.setTaskType(TaskType.TASK_TYPE_JOIN);
        joinTask.setTaskDefName(TaskType.TASK_TYPE_JOIN);
        joinTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        joinTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
        joinTask.setCorrelationId(workflowInstance.getCorrelationId());
        joinTask.setWorkflowType(workflowInstance.getWorkflowName());
        joinTask.setScheduledTime(System.currentTimeMillis());
        joinTask.setStartTime(System.currentTimeMillis());
        joinTask.setInputData(joinInput);
        joinTask.setTaskId(taskId);
        joinTask.setStatus(Task.Status.IN_PROGRESS);
        joinTask.setWorkflowTask(taskToSchedule);
        joinTask.setWorkflowPriority(workflowInstance.getPriority());

        return Collections.singletonList(joinTask);
    }
}

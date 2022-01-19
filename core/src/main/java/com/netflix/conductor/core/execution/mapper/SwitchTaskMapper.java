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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.execution.evaluators.Evaluator;

/**
 * An implementation of {@link TaskMapper} to map a {@link WorkflowTask} of type {@link
 * TaskType#SWITCH} to a List {@link Task} starting with Task of type {@link TaskType#SWITCH} which
 * is marked as IN_PROGRESS, followed by the list of {@link Task} based on the case expression
 * evaluation in the Switch task.
 */
@Component
public class SwitchTaskMapper implements TaskMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchTaskMapper.class);

    private final Map<String, Evaluator> evaluators;

    @Autowired
    public SwitchTaskMapper(Map<String, Evaluator> evaluators) {
        this.evaluators = evaluators;
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.SWITCH;
    }

    /**
     * This method gets the list of tasks that need to scheduled when the task to scheduled is of
     * type {@link TaskType#SWITCH}.
     *
     * @param taskMapperContext: A wrapper class containing the {@link WorkflowTask}, {@link
     *     WorkflowDef}, {@link Workflow} and a string representation of the TaskId
     * @return List of tasks in the following order:
     *     <ul>
     *       <li>{@link TaskType#SWITCH} with {@link Task.Status#IN_PROGRESS}
     *       <li>List of tasks based on the evaluation of {@link WorkflowTask#getEvaluatorType()}
     *           and {@link WorkflowTask#getExpression()} are scheduled.
     *       <li>In the case of no matching {@link WorkflowTask#getEvaluatorType()}, workflow will
     *           be terminated with error message. In case of no matching result after the
     *           evaluation of the {@link WorkflowTask#getExpression()}, the {@link
     *           WorkflowTask#getDefaultCase()} Tasks are scheduled.
     *     </ul>
     */
    @Override
    public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) {
        LOGGER.debug("TaskMapperContext {} in SwitchTaskMapper", taskMapperContext);
        List<Task> tasksToBeScheduled = new LinkedList<>();
        WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
        Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
        Map<String, Object> taskInput = taskMapperContext.getTaskInput();
        int retryCount = taskMapperContext.getRetryCount();
        String taskId = taskMapperContext.getTaskId();

        // get the expression to be evaluated
        String evaluatorType = taskToSchedule.getEvaluatorType();
        Evaluator evaluator = evaluators.get(evaluatorType);
        if (evaluator == null) {
            String errorMsg = String.format("No evaluator registered for type: %s", evaluatorType);
            LOGGER.error(errorMsg);
            throw new TerminateWorkflowException(errorMsg);
        }
        String evalResult = "" + evaluator.evaluate(taskToSchedule.getExpression(), taskInput);

        // QQ why is the case value and the caseValue passed and caseOutput passes as the same ??
        Task switchTask = new Task();
        switchTask.setTaskType(TaskType.TASK_TYPE_SWITCH);
        switchTask.setTaskDefName(TaskType.TASK_TYPE_SWITCH);
        switchTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        switchTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
        switchTask.setWorkflowType(workflowInstance.getWorkflowName());
        switchTask.setCorrelationId(workflowInstance.getCorrelationId());
        switchTask.setScheduledTime(System.currentTimeMillis());
        switchTask.getInputData().put("case", evalResult);
        switchTask.getOutputData().put("evaluationResult", Collections.singletonList(evalResult));
        switchTask.setTaskId(taskId);
        switchTask.setStartTime(System.currentTimeMillis());
        switchTask.setStatus(Task.Status.IN_PROGRESS);
        switchTask.setWorkflowTask(taskToSchedule);
        switchTask.setWorkflowPriority(workflowInstance.getPriority());
        tasksToBeScheduled.add(switchTask);

        // get the list of tasks based on the evaluated expression
        List<WorkflowTask> selectedTasks = taskToSchedule.getDecisionCases().get(evalResult);
        // if the tasks returned are empty based on evaluated result, then get the default case if
        // there is one
        if (selectedTasks == null || selectedTasks.isEmpty()) {
            selectedTasks = taskToSchedule.getDefaultCase();
        }
        // once there are selected tasks that need to proceeded as part of the switch, get the next
        // task to be
        // scheduled by using the decider service
        if (selectedTasks != null && !selectedTasks.isEmpty()) {
            WorkflowTask selectedTask =
                    selectedTasks.get(0); // Schedule the first task to be executed...
            // TODO break out this recursive call using function composition of what needs to be
            // done and then walk back the condition tree
            List<Task> caseTasks =
                    taskMapperContext
                            .getDeciderService()
                            .getTasksToBeScheduled(
                                    workflowInstance,
                                    selectedTask,
                                    retryCount,
                                    taskMapperContext.getRetryTaskId());
            tasksToBeScheduled.addAll(caseTasks);
            switchTask.getInputData().put("hasChildren", "true");
        }
        return tasksToBeScheduled;
    }
}

/**
 * Copyright 2018 Netflix, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.SystemTaskType;
import com.netflix.conductor.core.execution.TerminateWorkflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link TaskMapper} to map a {@link WorkflowTask} of type {@link TaskType#GOTO}
 * to a List {@link Task} starting with Task of type {@link SystemTaskType#GOTO} which is marked as IN_PROGRESS,
 * followed by the list of {@link Task} based on the case expression evaluation in the Decision task.
 */
public class GotoTaskMapper implements TaskMapper {

    Logger logger = LoggerFactory.getLogger(GotoTaskMapper.class);

    /**
     * This method gets the list of tasks that need to scheduled when the the task to scheduled is of type {@link TaskType#DECISION}.
     *
     * @param taskMapperContext: A wrapper class containing the {@link WorkflowTask}, {@link WorkflowDef}, {@link Workflow} and a string representation of the TaskId
     * @return List of tasks in the following order:
     * <ul>
     * <li>
     * {@link SystemTaskType#GOTO} with {@link Task.Status#IN_PROGRESS}
     * </li>
     * <li>
     * List of task based on the evaluation of {@link WorkflowTask#getGotoTask()} are scheduled.
     * </li>
     * </ul>
     */
    @Override
    public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) {
    	
        logger.debug("TaskMapperContext {} in GotoTaskMapper", taskMapperContext);
        List<Task> tasksToBeScheduled = new LinkedList<>();
        WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
        Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
        Map<String, Object> taskInput = taskMapperContext.getTaskInput();
        int retryCount = taskMapperContext.getRetryCount();
        int iterationCount = taskMapperContext.getIterationCount();
        String taskId = taskMapperContext.getTaskId();

        Task gotoTask = new Task();
        gotoTask.setTaskType(SystemTaskType.GOTO.name());
        gotoTask.setTaskDefName(SystemTaskType.GOTO.name());
        gotoTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        gotoTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
        gotoTask.setWorkflowType(workflowInstance.getWorkflowName());
        gotoTask.setCorrelationId(workflowInstance.getCorrelationId());
        gotoTask.setScheduledTime(System.currentTimeMillis());
        gotoTask.setInputData(taskInput);
        gotoTask.setTaskId(taskId);
        gotoTask.setRetryCount(retryCount);
        gotoTask.setStatus(Task.Status.IN_PROGRESS);
        gotoTask.setWorkflowTask(taskToSchedule);
        gotoTask.setIterationCount(iterationCount);
        tasksToBeScheduled.add(gotoTask);
        
        
        // get the next task to be executed from goto task
    	    WorkflowTask nextWorkflowTask = workflowInstance.getWorkflowDefinition().getTaskByRefName(taskToSchedule.getGotoTask());
    	    
    	    //get the current iterationCount of next task to be executed if it is already executed.
    	    Task nextTask = workflowInstance.getTaskByRefName(nextWorkflowTask.getTaskReferenceName());
    	    int nextTaskIterationCount = 0;
    	    
    	    if(nextTask != null)
    	    {
    	    		nextTaskIterationCount = nextTask.getIterationCount() + 1;
    	    }
    	    
    	    
        List<Task> nextTasks = taskMapperContext.getDeciderService().getTasksToBeScheduled(workflowInstance, nextWorkflowTask, retryCount, taskMapperContext.getRetryTaskId(), nextTaskIterationCount); //get next task to be scheduled with iterationCount incremented by one
    		
        if(!nextTasks.isEmpty()) { 
        	
    			Map<String, Object> gotoTaskInputMap = gotoTask.getInputData();
    			if(gotoTaskInputMap != null) {
    				replaceNextTasksInputWithGotoTaskInput(gotoTaskInputMap, nextTasks); // update next task's input with GOTO task's input if they have same input key
    			}
    			
    			tasksToBeScheduled.addAll(nextTasks);
        }
            
        return tasksToBeScheduled;
    }
    
    
	private void replaceNextTasksInputWithGotoTaskInput(Map<String, Object> gotoTaskInputMap, List<Task> nextTasks) {
		if (gotoTaskInputMap.size() == 0) {
			return;
		}

		for (Map.Entry<String, Object> entry : gotoTaskInputMap.entrySet()) {
			String keyToSearch = entry.getKey();
			Object valueToReplace = entry.getValue();

			for (Task t : nextTasks) {
				Map<String, Object> taskInputMap = t.getInputData();
				if (taskInputMap.containsKey(keyToSearch)) {
					taskInputMap.replace(keyToSearch, valueToReplace);
				}
			}

		}

	}

  
}

/*
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
package com.netflix.conductor.core.execution.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.tasks.EventWait;

public class EventWaitTaskMapper implements TaskMapper {

	public static final Logger logger = LoggerFactory.getLogger(EventWaitTaskMapper.class);

	private ParametersUtils parametersUtils;

	public EventWaitTaskMapper(ParametersUtils parametersUtils) {
		this.parametersUtils = parametersUtils;
	}

	@Override
	public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) {

		logger.debug("TaskMapperContext {} in EventTaskMapper", taskMapperContext);

		WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
		Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
		String taskId = taskMapperContext.getTaskId();

		taskToSchedule.getInputParameters().put("sink", taskToSchedule.getSink());
		Map<String, Object> eventWaitTaskInput = parametersUtils.getTaskInputV2(taskToSchedule.getInputParameters(),
				workflowInstance, taskId, null);
		String sink = (String) eventWaitTaskInput.get("sink");

		Task eventWaitTask = new Task();
		eventWaitTask.setTaskType(EventWait.NAME);
		eventWaitTask.setTaskDefName(taskToSchedule.getName());
		eventWaitTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		eventWaitTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
		eventWaitTask.setWorkflowType(workflowInstance.getWorkflowName());
		eventWaitTask.setCorrelationId(workflowInstance.getCorrelationId());
		eventWaitTask.setScheduledTime(System.currentTimeMillis());
		eventWaitTask.setEndTime(System.currentTimeMillis());
		eventWaitTask.setInputData(eventWaitTaskInput);
		eventWaitTask.getInputData().put("sink", sink);
		eventWaitTask.setTaskId(taskId);
		eventWaitTask.setStatus(Task.Status.SCHEDULED);
		eventWaitTask.setWorkflowTask(taskToSchedule);

		return Collections.singletonList(eventWaitTask);
	}
}
/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.ExecutionDAO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleksiy Lysak
 */

public class GetTaskData extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(GetTaskData.class);
	private final TypeReference MAP_TYPE = new TypeReference<HashMap<String, Object>>() {};
	private final ObjectMapper mapper = new ObjectMapper();

	public GetTaskData() {
		super("GET_TASK_DATA");
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		try {
			String workflowId = (String)task.getInputData().get("workflowId");
			if (StringUtils.isEmpty(workflowId))
				throw new IllegalArgumentException("No workflowId provided in parameters");

			String taskId = (String)task.getInputData().get("taskId");
			String taskRefName = (String)task.getInputData().get("taskRefName");

			if (StringUtils.isEmpty(taskId) && StringUtils.isEmpty(taskRefName))
				throw new IllegalArgumentException("No taskId/taskRefName provided in parameters");

			ExecutionDAO executionDao = executor.getExecutionDao();

			Task targetTask;
			if (StringUtils.isNotEmpty(taskId))
				targetTask = executionDao.getTask(taskId);
			else
				targetTask = executionDao.getTask(workflowId, taskRefName);

			if (targetTask == null) {
				String error = "No task found in workflow " + workflowId + " for ";
				if (StringUtils.isNotEmpty(taskId))
					throw new IllegalArgumentException(error + taskId);
				else
					throw new IllegalArgumentException(error + taskRefName);
			}

			Map<String, Object> outputData = mapper.convertValue(targetTask, MAP_TYPE);

			task.setOutputData(outputData);
			task.setStatus(Task.Status.COMPLETED);
		} catch (Exception e) {
			task.setStatus(Task.Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
	}
}
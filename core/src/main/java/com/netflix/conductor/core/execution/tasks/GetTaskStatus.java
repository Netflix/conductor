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

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.ExecutionDAO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Oleksiy Lysak
 */
public class GetTaskStatus extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(GetTaskStatus.class);

	public GetTaskStatus() {
		super("GET_TASK_STATUS");
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
				task.setStatus(Task.Status.FAILED);
				task.setReasonForIncompletion("No task found with id " + workflowId);
				return;
			}
			Task.Status status = targetTask.getStatus();
			if (status == null)
				throw new IllegalArgumentException("No status in the task " + targetTask.getTaskId());

			task.getOutputData().put("name", status.name());
			task.getOutputData().put("isTerminal", status.isTerminal());
			task.getOutputData().put("isSuccessful", status.isSuccessful());
			task.getOutputData().put("isRetriable", status.isRetriable());
			task.setStatus(Task.Status.COMPLETED);
		} catch (Exception e) {
			task.setStatus(Task.Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
			logger.error(e.getMessage(), e);
		}
	}
}

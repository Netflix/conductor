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
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleksiy Lysak
 *
 */
public class UpdateTask extends WorkflowSystemTask {
	private static Logger logger = LoggerFactory.getLogger(UpdateTask.class);
	private static final String STATUS_PARAMETER = "status";
	private static final String WORKFLOW_ID_PARAMETER = "workflowId";
	private static final String TASKREF_NAME_PARAMETER = "taskRefName";
	private static final String RESET_PARAMETER = "resetStartTime";
	private static final String OUTPUT_PARAMETER = "output";
	private static final String REASON_PARAMETER = "reason";
	public static final String NAME = "UPDATE_TASK";

	public UpdateTask() {
		super(NAME);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.COMPLETED);

		Map<String, Object> inputData = task.getInputData();
		String status = (String) inputData.get(STATUS_PARAMETER);
		String workflowId = (String) inputData.get(WORKFLOW_ID_PARAMETER);
		String taskRefName = (String) inputData.get(TASKREF_NAME_PARAMETER);
		if (StringUtils.isEmpty(status)) {
			task.setReasonForIncompletion("Missing '" + STATUS_PARAMETER + "' in input parameters");
			task.setStatus(Status.FAILED);
			return;
		}
		if (!status.equals(Status.COMPLETED.name())
			&& !status.equals(Status.FAILED.name())
			&& !status.equals(Status.IN_PROGRESS.name())) {
			task.setReasonForIncompletion("Invalid '" + STATUS_PARAMETER + "' value. Allowed COMPLETED/FAILED/IN_PROGRESS only");
			task.setStatus(Status.FAILED);
			return;
		}
		if (StringUtils.isEmpty(workflowId)) {
			task.setReasonForIncompletion("Missing '" + WORKFLOW_ID_PARAMETER + "' in input parameters");
			task.setStatus(Status.FAILED);
			return;
		}
		if (StringUtils.isEmpty(taskRefName)) {
			task.setReasonForIncompletion("Missing '" + TASKREF_NAME_PARAMETER + "' in input parameters");
			task.setStatus(Status.FAILED);
			return;
		}

		try {
			// Get the output map (optional) to be propagated to the target task
			Map<String, Object> output = (Map<String, Object>) inputData.get(OUTPUT_PARAMETER);
			if (output == null) {
				output = new HashMap<>();
			}

			Workflow targetWorkflow = executor.getWorkflow(workflowId, false);
			if (targetWorkflow == null) {
				task.setReasonForIncompletion("No workflow found with id " + workflowId);
				task.setStatus(Status.FAILED);
				return;
			}

			Task targetTask = executor.getTask(workflowId, taskRefName);
			if (targetTask == null) {
				task.setReasonForIncompletion("No task found with reference name " + taskRefName + ", workflowId " + workflowId);
				task.setStatus(Status.FAILED);
				return;
			}

			targetTask.setStatus(Status.valueOf(status));
			targetTask.setOutputData(output);

			TaskResult taskResult = new TaskResult(targetTask);
			taskResult.setResetStartTime(getResetStartTime(task));
			taskResult.setReasonForIncompletion(getReasonForIncompletion(task));
			taskResult.setUpdateOutput(MapUtils.isNotEmpty(output));

			executor.updateTask(taskResult);
		} catch (Exception e) {
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion("Unable to update task: " + e.getMessage());
			logger.error("Unable to update task: " + e.getMessage(), e);
		}
	}

	private boolean getResetStartTime(Task task) {
		Object obj = task.getInputData().get(RESET_PARAMETER);
		if (obj instanceof Boolean) {
			return (boolean) obj;
		} else if (obj instanceof String) {
			return Boolean.parseBoolean((String) obj);
		}
		return false;
	}

	private String getReasonForIncompletion(Task task) {
		if (!task.getInputData().containsKey(REASON_PARAMETER)) {
			return null;
		}
		Object obj = task.getInputData().get(REASON_PARAMETER);
		if (obj == null) {
			return null;
		}
		if (obj instanceof String) {
			return (String) obj;
		}
		return obj.toString();
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
	}
}

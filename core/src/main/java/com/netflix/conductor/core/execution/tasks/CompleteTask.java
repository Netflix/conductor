/**
 * Copyright 2016 Netflix, Inc.
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
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleksiy Lysak
 *
 */
public class CompleteTask extends WorkflowSystemTask {
	private static Logger logger = LoggerFactory.getLogger(CompleteTask.class);
	private static final String STATUS_PARAMETER = "status";
	private static final String WORKFLOW_ID_PARAMETER = "workflowId";
	private static final String TASKREF_NAME_PARAMETER = "taskRefName";
	private static final String OUTPUT_PARAMETER = "output";
	public static final String NAME = "COMPLETE_TASK";

	public CompleteTask() {
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
		} else if (!status.equals(Status.COMPLETED.name()) && !status.equals(Status.FAILED.name())) {
			task.setReasonForIncompletion("Invalid '" + STATUS_PARAMETER + "' value. Allowed COMPLETED/FAILED only");
			task.setStatus(Status.FAILED);
		} else if (StringUtils.isEmpty(workflowId)) {
			task.setReasonForIncompletion("Missing '" + WORKFLOW_ID_PARAMETER + "' in input parameters");
			task.setStatus(Status.FAILED);
		} else if (StringUtils.isEmpty(taskRefName)) {
			task.setReasonForIncompletion("Missing '" + TASKREF_NAME_PARAMETER + "' in input parameters");
			task.setStatus(Status.FAILED);
		}

		// Get the output map (optional) to be propagated to the target task
		Map<String, Object> output = (Map<String, Object>) inputData.get(OUTPUT_PARAMETER);
		if (output == null) {
			output = new HashMap<>();
		}

		Workflow targetWorkflow = executor.getWorkflow(workflowId, true);
		if (targetWorkflow == null) {
			task.getOutputData().put("error", "No workflow found with ID: " + workflowId);
			return;
		}

		Task targetTask = targetWorkflow.getTaskByRefName(taskRefName);
		if (targetTask == null) {
			task.getOutputData().put("error", "No task found with reference name: " + taskRefName + ", workflowId: " + workflowId);
			return;
		}

		targetTask.setStatus(Status.valueOf(status));
		targetTask.getOutputData().putAll(output);

		try {
			executor.updateTask(new TaskResult(targetTask));
		} catch (Exception e) {
			logger.error("Unable to complete task", e);
			task.getOutputData().put("error", "Unable to complete task. " + e.getMessage());
		}
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
	}
}

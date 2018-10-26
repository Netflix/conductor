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
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang.StringUtils;

/**
 * @author Oleksiy Lysak
 *
 */
public class Fail extends WorkflowSystemTask {
	private static final String REASON_PARAMETER = "reason";
	private static final String STATUS_PARAMETER = "status";
	private static final String SUPPRESS_RESTART_PARAMETER = "suppressRestart";

	public static final String NAME = "FAIL";

	public Fail() {
		super(NAME);
	}
	
	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		String reason = (String)task.getInputData().get(REASON_PARAMETER);
		if (StringUtils.isEmpty(reason)) {
			reason = "Missing '" + REASON_PARAMETER + "' in input parameters";
		}
		String status = (String)task.getInputData().get(STATUS_PARAMETER);

		Status taskStatus = Status.FAILED;
		if (StringUtils.isNotEmpty(status)) {
			taskStatus = Status.valueOf(status);
		}
		task.setReasonForIncompletion(reason);
		task.setStatus(taskStatus);

		if (isSuppressRestart(task)) {
			workflow.getOutput().put(SUPPRESS_RESTART_PARAMETER, true);
		}
	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		return true;
	}
	
	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
	}

	private boolean isSuppressRestart(Task task) {
		Object obj = task.getInputData().get(SUPPRESS_RESTART_PARAMETER);
		if (obj instanceof Boolean) {
			return (boolean)obj;
		} else if (obj instanceof String) {
			return Boolean.parseBoolean((String)obj);
		}
		return false;
	}
}

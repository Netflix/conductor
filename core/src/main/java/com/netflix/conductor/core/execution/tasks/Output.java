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
import org.apache.commons.collections.MapUtils;

import java.util.Map;

/**
 * @author Oleksiy Lysak
 *
 */
public class Output extends WorkflowSystemTask {

	public static final String NAME = "OUTPUT";

	public Output() {
		super(NAME);
	}
	
	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor provider) {
		task.setStatus(Status.COMPLETED);
		Map<String, Object> payload = task.getInputData();
		if (MapUtils.isNotEmpty(payload)) {
			// That causes conductor to assign all tasks output to the workflow output
			task.getOutputData().putAll(payload);
		}
	}
}

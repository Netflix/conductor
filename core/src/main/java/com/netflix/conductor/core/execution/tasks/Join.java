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
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.QueueUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Viren
 *
 */
public class Join extends WorkflowSystemTask {
	private static Logger logger = LoggerFactory.getLogger(Join.class);
	private ParametersUtils pu = new ParametersUtils();

	public Join() {
		super("JOIN");
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {

		boolean allDone = true;
		boolean hasFailures = false;
		String failureReason = "";
		List<String> joinOn = (List<String>) task.getInputData().get("joinOn");
		for (String joinOnRef : joinOn) {
			Task forkedTask = workflow.getTaskByRefName(joinOnRef);
			if (forkedTask == null) {
				//Task is not even scheduled yet
				allDone = false;
				break;
			}
			Status taskStatus = forkedTask.getStatus();
			hasFailures = !taskStatus.isSuccessful();
			if (hasFailures) {
				failureReason += forkedTask.getReasonForIncompletion() + " ";
			}
			task.getOutputData().put(joinOnRef, forkedTask.getOutputData());
			allDone = taskStatus.isTerminal();
			if (!allDone || hasFailures) {
				break;
			}
		}
		if (allDone || hasFailures) {
			if (hasFailures) {
				task.setReasonForIncompletion(failureReason);
				task.setStatus(Status.FAILED);
			} else {
				task.setStatus(Status.COMPLETED);
			}
			return true;
		}
		// Otherwise execute conditional join
		List<String> joinConditions = (List<String>) task.getInputData().get("joinOnConditions");
		if (CollectionUtils.isNotEmpty(joinConditions)) {
			Map<String, Object> inputParameters = (Map<String, Object>) task.getInputData().get("inputParameters");
			Map<String, Object> payload = pu.getTaskInputV2(inputParameters, workflow, null, null);
			boolean allSuccess = false;
			for (String condition : joinConditions) {
				String evaluated = ScriptEvaluator.evalJq(condition, payload);
				allSuccess = "true".equalsIgnoreCase(evaluated);
				if (!allSuccess) {
					break;
				}
			}
			if (allSuccess) {
				for (String joinOnRef : joinOn) {
					Task forkedTask = workflow.getTaskByRefName(joinOnRef);
					// Cancel the task if that still running
					if (!forkedTask.getStatus().isTerminal()) {
						forkedTask.setStatus(Status.COMPLETED);
						provider.updateTask(forkedTask);

						String queueName = QueueUtils.getQueueName(task);
						provider.getQueueDao().remove(queueName, forkedTask.getTaskId());
					}
					task.getOutputData().put(joinOnRef, forkedTask.getOutputData());
				}
				task.setStatus(Status.COMPLETED);
				return true;
			}
		}
		return false;
	}

}

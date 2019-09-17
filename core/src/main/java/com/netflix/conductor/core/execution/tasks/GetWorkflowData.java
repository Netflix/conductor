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
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Oleksiy Lysak
 */

public class GetWorkflowData extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(GetWorkflowData.class);
	private final TypeReference MAP_TYPE = new TypeReference<HashMap<String, Object>>() {};
	private final ObjectMapper mapper = new ObjectMapper();

	public GetWorkflowData(ObjectMapper mapper) {
		super("GET_WORKFLOW_DATA");
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		try {
			String workflowId = (String)task.getInputData().get("workflowId");
			if (StringUtils.isEmpty(workflowId))
				throw new IllegalArgumentException("No workflowId provided in parameters");

			ExecutionDAO executionDao = executor.getExecutionDao();

			Workflow targetWorkflow = executionDao.getWorkflow(workflowId, includeTasks(task));
			if (targetWorkflow == null)
				throw new IllegalArgumentException("No workflow found with id " + workflowId);

			Map<String, Object> outputData = mapper.convertValue(targetWorkflow, MAP_TYPE);

			task.setOutputData(outputData);
			task.setStatus(Task.Status.COMPLETED);
		} catch (Exception e) {
			task.setStatus(Task.Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
	}

	private boolean includeTasks(Task task) {
		Object obj = task.getInputData().get("includeTasks");
		if (obj instanceof Boolean) {
			return (boolean) obj;
		} else if (obj instanceof String) {
			return Boolean.parseBoolean((String) obj);
		}
		return false;
	}
}
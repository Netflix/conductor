package com.netflix.conductor.contribs.taskupdate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.*;
import org.apache.commons.collections.MapUtils;
import com.netflix.conductor.core.utils.TaskUtils;
@Singleton
public class TaskUpdateHandler implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(TaskUpdateHandler.class);
	private static final String JQ_GET_WFID_URN = ".Urns[] | select(startswith(\"urn:deluxe:conductor:manualTaskworkflow:\")) | split(\":\") [4]";
	private final WorkflowExecutor executor;
	private final ObjectMapper mapper;

	@Inject
	public TaskUpdateHandler(WorkflowExecutor executor, ObjectMapper mapper) {
		this.executor = executor;
		this.mapper = mapper;
	}

	@Override
	public ArrayList<String> handle(EventHandler.Action action, Object payload, EventExecution ee) throws Exception {
		Set<String> output = new HashSet<>();
		ActionParams params = mapper.convertValue(action.getJava_action().getInputParameters(), ActionParams.class);
		if (StringUtils.isEmpty(params.taskRefName)) {
			throw new IllegalStateException("No taskRefName defined in parameters");
		}
		Map<String, Object> eventpayload = mapper.convertValue(payload, Map.class);
		Map<String, Object> data =(Map<String, Object>) eventpayload.get("data");
		List<Map<String, Object>> tasksUpdated = (List<Map<String, Object>>) data.get("TasksUpdated");

		tasksUpdated.forEach(item -> {

				String workflowJq = StringUtils.defaultIfEmpty(params.workflowIdJq, JQ_GET_WFID_URN);
			    try {
				String workflowId = ScriptEvaluator.evalJq(workflowJq, item);
				if (StringUtils.isEmpty(workflowId)) {
					logger.debug("Skipping. No workflowId provided in urns");
					return;
				}
			    Workflow workflow = executor.getWorkflow(workflowId, true);
				Task.Status taskStatus;
				if (StringUtils.isNotEmpty(params.status)) {
					// Get an evaluating which might result in error or empty response
					Map<String, Object> UpdatedTask =(Map<String, Object>) item.get("UpdatedTask");
					String status = ScriptEvaluator.evalJq(params.status, item);
					if (StringUtils.isEmpty(status))
						throw new RuntimeException("Unable to determine status. Check mapping and payload");

					// If mapping exists - take the task status from mapping
					if (MapUtils.isNotEmpty(params.statuses)) {
						status = params.statuses.get(status);
						taskStatus = TaskUtils.getTaskStatus(status);
					} else {
						taskStatus = TaskUtils.getTaskStatus(status);
					}
				} else {
					taskStatus = Task.Status.COMPLETED;
				}

				if (workflow == null) {
					logger.debug("Skipping. No workflow found for given id " + workflowId);
					return;
				}

				if (workflow.getStatus().isTerminal()) {
					logger.debug("Skipping. Target workflow is already " + workflow.getStatus().name()
							+ ", workflowId=" + workflow.getWorkflowId()
							+ ", contextUser=" + workflow.getContextUser()
							+ ", correlationId=" + workflow.getCorrelationId());
					return;
				}

				Task task = workflow.getTaskByRefName(params.taskRefName);
				if (task == null) {
					logger.debug("Skipping. No task " + params.taskRefName + " found in workflow"
							+ ", workflowId=" + workflow.getWorkflowId()
							+ ", contextUser=" + workflow.getContextUser()
							+ ", correlationId=" + workflow.getCorrelationId());
					return;
				}

				if (task.getStatus().isTerminal()) {
					logger.debug("Skipping. Target task " + task + " is already finished. "
							+ ", workflowId=" + workflow.getWorkflowId()
							+ ", contextUser=" + workflow.getContextUser()
							+ ", correlationId=" + workflow.getCorrelationId());
					return;
				}


				task.setStatus(taskStatus);

				TaskResult taskResult = new TaskResult(task);
				taskResult.getOutputData().put("payload", payload);
				executor.updateTask(taskResult);
				output.add(workflow.getWorkflowId());
			}
			catch (Exception ex) {
				logger.error("Batch task update failed");
			}
		});
		return new ArrayList<>(output);
	}

	// Keep fields public!
	public static class ActionParams {
		public String taskRefName;
		public String workflowIdJq;
		public String status;
		public Map<String, String> statuses;
	}
}

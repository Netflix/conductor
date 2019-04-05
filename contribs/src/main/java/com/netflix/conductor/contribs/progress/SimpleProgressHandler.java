package com.netflix.conductor.contribs.progress;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import static com.netflix.conductor.common.run.Workflow.WorkflowStatus.*;

@Singleton
public class SimpleProgressHandler implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(SimpleProgressHandler.class);
	private static final String JQ_GET_WFID_URN = ".urns[] | select(startswith(\"urn:deluxe:conductor:workflow:\")) | split(\":\") [4]";
	private final WorkflowExecutor executor;
	private final ObjectMapper mapper;

	@Inject
	public SimpleProgressHandler(WorkflowExecutor executor, ObjectMapper mapper) {
		this.executor = executor;
		this.mapper = mapper;
	}

	@Override
	public List<String> handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		ActionParams params = mapper.convertValue(action.getJava_action().getInputParameters(), ActionParams.class);
		if (StringUtils.isEmpty(params.taskRefName)) {
			throw new IllegalStateException("No taskRefName defined in parameters");
		}

		String workflowId = ScriptEvaluator.evalJq(JQ_GET_WFID_URN, payload);
		if (StringUtils.isEmpty(workflowId)) {
			logger.debug("Skipping. No workflowId provided in urns");
			return Collections.emptyList();
		}

		Workflow workflow = executor.getWorkflow(workflowId, true);
		if (workflow == null) {
			logger.debug("Skipping. No workflow found for given id " + workflowId);
			return Collections.emptyList();
		}

		if (workflow.getStatus() == COMPLETED
			|| workflow.getStatus() == FAILED
			|| workflow.getStatus() == CANCELLED) {
			logger.debug("Skipping. Target workflow is already " + workflow.getStatus().name()
				+ ", workflowId=" + workflowId
				+ ", contextUser=" + workflow.getContextUser()
				+ ", correlationId=" + workflow.getCorrelationId());
			return Collections.emptyList();
		}

		Task task = workflow.getTaskByRefName(params.taskRefName);
		if (task == null) {
			logger.debug("Skipping. No task " + params.taskRefName + " found in workflow"
				+ ", workflowId=" + workflowId
				+ ", contextUser=" + workflow.getContextUser()
				+ ", correlationId=" + workflow.getCorrelationId());
			return Collections.emptyList();
		}

		if (task.getStatus().isTerminal()) {
			logger.debug("Skipping. Target " + task + " is already finished. "
				+ ", workflowId=" + workflowId
				+ ", contextUser=" + workflow.getContextUser()
				+ ", correlationId=" + workflow.getCorrelationId());
			return Collections.emptyList();
		}

		// Mae sure it is in progress
		task.setStatus(Task.Status.IN_PROGRESS);

		TaskResult taskResult = new TaskResult(task);
		taskResult.setResetStartTime(params.resetStartTime);
		executor.updateTask(taskResult);
		logger.debug("Task " + task + " has been updated"
			+ ", workflowId=" + workflowId
			+ ", contextUser=" + workflow.getContextUser()
			+ ", correlationId=" + workflow.getCorrelationId());

		return Collections.singletonList(workflowId);
	}

	// Keep it public!
	public static class ActionParams {
		public String taskRefName;
		public boolean resetStartTime = true;
	}
}

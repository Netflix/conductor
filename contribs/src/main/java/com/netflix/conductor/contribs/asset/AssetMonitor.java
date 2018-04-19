package com.netflix.conductor.contribs.asset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.dao.ExecutionDAO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class AssetMonitor implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(AssetMonitor.class);
	private final WorkflowExecutor executor;
	private final ExecutionDAO edao;
	private final ObjectMapper om;


	@Inject
	public AssetMonitor(WorkflowExecutor executor, ExecutionDAO edao, ObjectMapper om) {
		this.executor = executor;
		this.edao = edao;
		this.om = om;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		Map<String, Object> params = action.getJava_action().getInputParameters();
		ActionParameters actionParameters = om.convertValue(params, ActionParameters.class);

		String workflowName = actionParameters.getWorkflowName();
		if (StringUtils.isEmpty(workflowName)) {
			throw new RuntimeException("No workflow defined");
		}

		// Get the assetId from the nats message
		String assetId = ScriptEvaluator.evalJq(".data.assetKeys.assetId", payload);

		// Make sure assetId present in the message
		if (StringUtils.isEmpty(assetId)) {
			logger.info("Doing noting as no assetId in the message " + messageId);
			return;
		}

		// Find running WFs
		for (Workflow workflow : executor.getRunningWorkflows(workflowName)) {
			Task deliverableJoin = workflow.getTasks().stream()
					.filter(task -> task.getReferenceTaskName().equalsIgnoreCase("deliverable_join"))
					.findFirst().orElse(null);

			// Null means that the task not even scheduled. Rare case but might happen. Just logging that information
			if (deliverableJoin == null) {
				logger.error("The workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + " not in the right state to handle the message " + messageId);
				continue;
			}

			// All deliverables are completed
			if (deliverableJoin.getStatus() == Task.Status.COMPLETED) {
				checkForCancellation(workflow, deliverableJoin, assetId);
			} else if (deliverableJoin.getStatus() == Task.Status.IN_PROGRESS) {
				// Some might be completed and some still running
				checkForRestart(workflow, assetId);
			} else {
				logger.warn("The deliverable_join task not in COMPLETED/IN_PROGRESS status for the workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + ", messageId=" + messageId);
			}
		}
	}

	private void checkForCancellation(Workflow workflow, Task task, String assetId) throws Exception {
		// Check asset id in the deliverable outputs
		List<Object> assetIds = ScriptEvaluator.evalJqAsList(".[].input[].atlasData.id", task.getOutputData());

		// Cancel workflow "Invalidate Asset" if any of them has assetId in the params
		if (assetIds != null && assetIds.contains(assetId)) {
			executor.cancelWorkflow(workflow, Collections.emptyMap(), "Invalidate Asset");
		}
	}

	private void checkForRestart(Workflow workflow, String assetId) throws Exception {
		// Get the list of completed deliverables.
		List<Task> completed = workflow.getTasks().stream()
				.filter(task -> task.getTaskType().equalsIgnoreCase(SubWorkflow.NAME))
				.filter(task -> task.getReferenceTaskName().startsWith("deliverable"))
				.filter(task -> task.getStatus().equals(Task.Status.COMPLETED))
				.collect(Collectors.toList());

		// If any of them has assetId - rerun task
		for (Task task : completed) {
			// Check asset id in the sub-workflow input array which must be in the task output
			List<Object> assetIds = ScriptEvaluator.evalJqAsList(".input[].atlasData.id", task.getOutputData());
			if (assetIds != null && assetIds.contains(assetId)) {
				String subWorkflowId = (String) task.getOutputData().get("subWorkflowId");
				logger.info("Asset matches. Restarting sub-workflow " + subWorkflowId);

				task.setStartTime(System.currentTimeMillis());
				task.setEndTime(0);
				task.setRetried(false);
				task.setStatus(Task.Status.IN_PROGRESS);
				edao.updateTask(task);

				Workflow subWorkflow = executor.getWorkflow(subWorkflowId, false);
				executor.rewind(subWorkflowId, subWorkflow.getHeaders());
			}
		}
	}

	public static class ActionParameters {
		private String workflowName;

		String getWorkflowName() {
			return workflowName;
		}

		public void setWorkflowName(String workflowName) {
			this.workflowName = workflowName;
		}
	}
}
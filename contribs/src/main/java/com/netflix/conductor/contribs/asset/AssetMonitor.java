package com.netflix.conductor.contribs.asset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.dao.ExecutionDAO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
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
		ActionParameters params = om.convertValue(action.getJava_action().getInputParameters(), ActionParameters.class);

		String workflowName = params.getWorkflowName();
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
				sendAlertMessage(workflow, deliverableJoin, assetId, params, payload);
			} else if (deliverableJoin.getStatus() == Task.Status.IN_PROGRESS) {
				// Some might be completed and some still running
				checkForRestart(workflow, assetId);
			} else {
				logger.warn("The deliverable_join task not in COMPLETED/IN_PROGRESS status for the workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + ", messageId=" + messageId);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void sendAlertMessage(Workflow workflow, Task task, String assetId, ActionParameters params, Object payload) throws Exception {
		// Check asset id in the deliverable outputs
		List<Object> assetIds = ScriptEvaluator.evalJqAsList(".[].input[].atlasData.id", task.getOutputData());

		// Cancel workflow "Invalidate Asset" if any of them has assetId in the params
		if (assetIds != null && assetIds.contains(assetId)) {
			ParametersUtils pu = new ParametersUtils();

			String sink = params.getAlertMessage().getSink();

			Map<String, Object> event = (Map<String, Object>)payload;
			Map<String, Map<String, Object>> defaults = new HashMap<>();
			defaults.put("event", event);

			Map<String, Object> input = params.getAlertMessage().getInputParameters();
			Map<String, Object> message = pu.getTaskInputV2(input, defaults, workflow, UUID.randomUUID().toString(), null, null);

			Message msg = new Message();
			msg.setId(UUID.randomUUID().toString());
			msg.setPayload(om.writeValueAsString(message));

			ObservableQueue queue = EventQueues.getQueue(sink, true);
			if (queue == null) {
				throw new RuntimeException("sendAlertMessage. No queue found for " + sink);
			}

			queue.publish(Collections.singletonList(msg));
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
		private AlertMessage alertMessage;

		String getWorkflowName() {
			return workflowName;
		}

		public void setWorkflowName(String workflowName) {
			this.workflowName = workflowName;
		}

		public AlertMessage getAlertMessage() {
			return alertMessage;
		}

		public void setAlertMessage(AlertMessage alertMessage) {
			this.alertMessage = alertMessage;
		}
	}

	public static class AlertMessage {
		private String sink;
		private Map<String, Object> inputParameters;

		public String getSink() {
			return sink;
		}

		public void setSink(String sink) {
			this.sink = sink;
		}

		public Map<String, Object> getInputParameters() {
			return inputParameters;
		}

		public void setInputParameters(Map<String, Object> inputParameters) {
			this.inputParameters = inputParameters;
		}
	}
}
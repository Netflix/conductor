package com.netflix.conductor.contribs.asset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.FindUpdateAction;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.dao.ExecutionDAO;
import org.apache.commons.collections.MapUtils;
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

	/**
	 * Complex event handler
	 *
	 * 1. Find/update action. Find 'source atlas wait'. List of wf ids in the result are being updated
	 * 2. Find delivery process workflows.
	 * 3. For each from step 2:
	 * 	 3.1. Find 'deliverable_join'.
	 * 	 3.2. If not found, log an error
	 * 	 3.3. completed? - go to 4
	 * 	 3.4. in-progress? - go to 5
	 * 4. Compare assetId. If assetId present in the workflow - send the nats alert message
	 * 5. Find deliverables (action source wait), for each:
	 *   5.1 Find the child 'source atlas wait' sub-workflow id
	 *   5.2 If the sub-workflow id is present in the step #1 - do nothing as that workflow recently processed by step #1
	 *   5.3 If the sub-workflow id is NOT present in the step #1 - compare titleKeys for "workflow inputs" against "nats message"
	 * 6. If matches, check the deliverable workflow status:
	 *   6.1 completed - restart it.
	 * 	 6.2 in-progress - reset it. The sub-workflow will be restarted automatically in some time.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public List<Object> handle(EventHandler.Action action, Object payload, String event, String messageId) throws Exception {
		ActionParameters params = om.convertValue(action.getJava_action().getInputParameters(), ActionParameters.class);

		// TODO Validate parameters
		String workflowName = params.getRerunWorkflow().getWorkflowName();

		// Step 1
		FindUpdateAction findUpdateJava = new FindUpdateAction(executor);
		EventHandler.Action act = new EventHandler.Action();
		act.setFind_update(params.getFindUpdate());
		act.setAction(EventHandler.Action.Type.find_update);
		List<String> sourceAtlasWaitIds = findUpdateJava.handleInternal(act, payload, event, messageId);

		// Step 2
		List<Workflow> workflows = executor.getRunningWorkflows(workflowName);

		// Step 3
		for (Workflow workflow : workflows) {
			// Step 3.1
			Task actionCheck = workflow.getTasks().stream()
				.filter(task -> task.getReferenceTaskName().equalsIgnoreCase("action_check"))
				.findFirst().orElse(null);

			Task joinTask = workflow.getTasks().stream()
					.filter(task -> task.getReferenceTaskName().equalsIgnoreCase("deliverable_join"))
					.findFirst().orElse(null);

			// Step 3.2
			if (actionCheck == null) {
				logger.debug("The workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + " not in the right state (no action_check) to handle the message " + messageId);
				continue;
			} else {
				if ("METADATA".equals(actionCheck.getInputData().get("case"))) {
					logger.trace("Skipping workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + " as it is METADATA");
					continue;
				}
			}

			// Null means that the task not even scheduled. Rare case but might happen. Just logging that information
			if (joinTask == null) {
				logger.debug("The workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + " not in the right state(no deliverable_join) to handle the message " + messageId);
				continue;
			}

			// All deliverables are completed
			if (joinTask.getStatus() == Task.Status.COMPLETED) { // 3.3
				checkForAlertMessage(params, workflow, joinTask, payload, messageId);
			} else if (joinTask.getStatus() == Task.Status.IN_PROGRESS) { // 3.4
				checkForRestart(params, workflow, payload, messageId, sourceAtlasWaitIds);
			} else {
				logger.warn("The deliverable_join task not in COMPLETED/IN_PROGRESS status for the workflow " + workflow.getWorkflowId() + ", correlationId=" + workflow.getCorrelationId() + ", messageId=" + messageId);
			}
		}
		return new ArrayList<>(sourceAtlasWaitIds);
	}

	// Step 4
	@SuppressWarnings("unchecked")
	private void checkForAlertMessage(ActionParameters params, Workflow workflow, Task task, Object payload, String messageId) throws Exception {
		// Get the assetId from the nats message
		String assetId = ScriptEvaluator.evalJq(".data.assetKeys.assetId", payload);

		// Make sure assetId present in the message
		if (StringUtils.isEmpty(assetId)) {
			logger.debug("Doing noting as no assetId in the message " + messageId);
			return;
		}

		// Check asset id in the deliverable outputs
		List<Object> assetIds = ScriptEvaluator.evalJqAsList(".[].input[].atlasData.id", task.getOutputData());

		// Send "Invalidate Asset" message if any of them has assetId in the params
		if (assetIds != null && assetIds.contains(assetId)) {
			ParametersUtils pu = new ParametersUtils();

			String sink = params.getSendAlert().getSink();

			Map<String, Object> event = (Map<String, Object>)payload;
			Map<String, Map<String, Object>> defaults = new HashMap<>();
			defaults.put("event", event);

			Map<String, Object> input = params.getSendAlert().getInputParameters();
			Map<String, Object> message = pu.getTaskInputV2(input, defaults, workflow, UUID.randomUUID().toString(), null, null);

			Message msg = new Message();
			msg.setId(UUID.randomUUID().toString());
			msg.setPayload(om.writeValueAsString(message));

			ObservableQueue queue = EventQueues.getQueue(sink, true);
			if (queue == null) {
				throw new RuntimeException("checkForAlertMessage. No queue found for " + sink);
			}

			queue.publish(Collections.singletonList(msg));
		}
	}

	// Step 5
	private void checkForRestart(ActionParameters params, Workflow workflow, Object payload, String messageId, List<String> waitWfIds) throws Exception {
		RerunWorkflowParam rerunWorkflow = params.getRerunWorkflow();

		// Get the list of deliverables.
		List<Task> deliverables = workflow.getTasks().stream()
				.filter(task -> task.getTaskType().equalsIgnoreCase(SubWorkflow.NAME))
				.filter(task -> task.getReferenceTaskName().startsWith("deliverable"))
				.collect(Collectors.toList());

		// Evaluate the message against the message match fields
		Map<String, Object> messageParameters = ScriptEvaluator.evaluateMap(rerunWorkflow.getMessageParameters(), payload);
		if (MapUtils.isEmpty(messageParameters)) {
			logger.error("No message parameters evaluated for " + messageId);
			return;
		}

		for (Task task : deliverables) {
			// Action Source Wait sub-workflow
			String actionSubWorkflowId = (String) task.getOutputData().get("subWorkflowId");
			if (StringUtils.isEmpty(actionSubWorkflowId)) {
				continue;
			}

			Workflow actionSubWorkflow = executor.getWorkflow(actionSubWorkflowId, true);
			if (actionSubWorkflow == null) {
				logger.debug("No workflow found with id " + actionSubWorkflowId + ", skipping " + task);
				continue;
			}

			// Find the source wait sub-flow task
			Task sourceWaitTask = actionSubWorkflow.getTasks().stream()
					.filter(t -> t.getTaskType().equalsIgnoreCase(SubWorkflow.NAME))
					.filter(t -> t.getReferenceTaskName().equalsIgnoreCase("sourcewaitsubflow"))
					.findFirst().orElse(null);

			// It might not exist (different flow)
			if (sourceWaitTask != null) {
				String sourceWaitSubId = (String) sourceWaitTask.getOutputData().get("subWorkflowId");

				// Do nothing if that sub-id presents in the list of recently updated
				if (waitWfIds.contains(sourceWaitSubId)) {
					logger.debug("Asset has been recently updated in sub-workflow " + sourceWaitSubId);
					continue;
				}
			}

			// Evaluate the workflow input map against the workflow match fields
			Map<String, Object> workflowParameters = ScriptEvaluator.evaluateMap(rerunWorkflow.getWorkflowParameters(), task.getInputData());
			if (MapUtils.isEmpty(workflowParameters)) {
				continue;
			}

			// All fields from the messageParameters have to be in the workflowParameters
			boolean anyMissed = messageParameters.keySet().stream().anyMatch(item -> !workflowParameters.containsKey(item));
			if (anyMissed) {
				continue;
			}

			// Skip if any of the messageParameters does not match the workflowParameters
			boolean anyNotEqual = messageParameters.entrySet().stream().anyMatch(entry -> {
				Object wfValue = workflowParameters.get(entry.getKey());
				Object msgValue = entry.getValue();
				return !(Objects.nonNull(wfValue)
						&& Objects.nonNull(msgValue)
						&& msgValue.equals(wfValue));
			});
			if (anyNotEqual) {
				continue;
			}

			// Move on reset/restart the sub-workflow
			task.setStartTime(System.currentTimeMillis());
			task.setEndTime(0);
			task.setRetried(false);
			task.setStatus(Task.Status.IN_PROGRESS);
			edao.updateTask(task);

			// reset sub-workflow, it will be restarted automatically
			if (actionSubWorkflow.getStatus().equals(Workflow.WorkflowStatus.RUNNING)) {
				logger.debug("Asset matches. Resetting sub-workflow " + actionSubWorkflowId);
				executor.reset(actionSubWorkflowId, "Asset matched. Has been reset by incoming message " + messageId);
			} else {
				logger.debug("Asset matches. Restarting sub-workflow " + actionSubWorkflowId);
				executor.rewind(actionSubWorkflowId, actionSubWorkflow.getCorrelationId());
			}
		}
	}

	private static class ActionParameters {
		private EventHandler.FindUpdate findUpdate;
		private RerunWorkflowParam rerunWorkflow;
		private SendAlertParam sendAlert;

		public EventHandler.FindUpdate getFindUpdate() {
			return findUpdate;
		}

		public void setFindUpdate(EventHandler.FindUpdate findUpdate) {
			this.findUpdate = findUpdate;
		}

		public RerunWorkflowParam getRerunWorkflow() {
			return rerunWorkflow;
		}

		public void setRerunWorkflow(RerunWorkflowParam rerunWorkflow) {
			this.rerunWorkflow = rerunWorkflow;
		}

		public SendAlertParam getSendAlert() {
			return sendAlert;
		}

		public void setSendAlert(SendAlertParam sendAlert) {
			this.sendAlert = sendAlert;
		}
	}

	private static class RerunWorkflowParam {
		private String workflowName;
		private Map<String, String> messageParameters;
		private Map<String, String> workflowParameters;

		public String getWorkflowName() {
			return workflowName;
		}

		public void setWorkflowName(String workflowName) {
			this.workflowName = workflowName;
		}

		public Map<String, String> getMessageParameters() {
			return messageParameters;
		}

		public void setMessageParameters(Map<String, String> messageParameters) {
			this.messageParameters = messageParameters;
		}

		public Map<String, String> getWorkflowParameters() {
			return workflowParameters;
		}

		public void setWorkflowParameters(Map<String, String> workflowParameters) {
			this.workflowParameters = workflowParameters;
		}
	}

	private static class SendAlertParam {
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
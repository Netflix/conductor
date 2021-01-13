package com.netflix.conductor.contribs.asset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.JavaEventAction;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.utils.TaskUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@Singleton
public class MetadataAtlasMatchAction implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(MetadataAtlasMatchAction.class);
	private final WorkflowExecutor executor;
	private final ObjectMapper mapper;

	@Inject
	public MetadataAtlasMatchAction(WorkflowExecutor executor, ObjectMapper mapper) {
		this.executor = executor;
		this.mapper = mapper;
	}

	@Override
	public List<String> handle(EventHandler.Action action, Object payload, EventExecution ee) throws Exception {
		Set<String> output = new HashSet<>();
		ActionParams params = mapper.convertValue(action.getJava_action().getInputParameters(), ActionParams.class);

		// Task status is completed by default. It either can be a constant or expression
		Task.Status taskStatus;
		if (isNotEmpty(params.status)) {
			// Get an evaluating which might result in error or empty response
			String status = ScriptEvaluator.evalJq(params.status, payload);
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

		String atlasId = ScriptEvaluator.evalJq(params.atlasId, payload);
		if (StringUtils.isEmpty(atlasId)) {
			logger.debug("Skipping as no atlasId present in " + payload);
			return Collections.emptyList();
		}

		String versionId = ScriptEvaluator.evalJq(params.versionId, payload);

		// Get the current logging context (owner)
		String ndcValue = NDC.peek();

		Set<String> tags = generateTags(atlasId, versionId);
		List<Task> tasks = executor.getPendingTasksByTags(Wait.NAME, tags);

		boolean taskNamesDefined = CollectionUtils.isNotEmpty(params.taskRefNames);
		tasks.parallelStream().forEach(task -> {
			boolean ndcCleanup = false;
			try {
				if (StringUtils.isEmpty(NDC.peek())) {
					ndcCleanup = true;
					NDC.push(ndcValue);
				}

				if (!task.getInputData().containsKey("referenceKeys")) {
					return;
				}

				if (taskNamesDefined && !params.taskRefNames.contains(task.getReferenceTaskName())) {
					return;
				}

				Workflow workflow = executor.getWorkflow(task.getWorkflowInstanceId(), false);
				if (workflow == null) {
					logger.debug("No workflow found with id " + task.getWorkflowInstanceId() + ", skipping " + task);
					return;
				}

				if (workflow.getStatus().isTerminal()) {
					return;
				}
				Object taskReferenceKeys = task.getInputData().get("referenceKeys");
				if (taskReferenceKeys == null) {
					return;
				}
				if (!(taskReferenceKeys instanceof List)) {
					logger.warn("Task input referenceKeys is not a list for " + task);
					return;
				}
				List<ReferenceKey> taskRefKeys = mapper.convertValue(taskReferenceKeys, new TypeReference<List<ReferenceKey>>() {
				});

				// Array match
				if (!matches(taskRefKeys, atlasId, versionId)) {
					return;
				}

				//Otherwise update the task as we found it
				task.setStatus(taskStatus);
				task.getOutputData().put("conductor.event.name", ee.getEvent());
				task.getOutputData().put("conductor.event.payload", payload);
				task.getOutputData().put("conductor.event.messageId", ee.getMessageId());
				logger.debug("Updating task " + task + ". workflowId=" + workflow.getWorkflowId()
					+ ",correlationId=" + workflow.getCorrelationId()
					+ ",traceId=" + workflow.getTraceId()
					+ ",contextUser=" + workflow.getContextUser()
					+ ",messageId=" + ee.getMessageId()
					+ ",payload=" + payload);

				// Set the reason if task failed. It should be provided in the event
				if (Task.Status.FAILED.equals(taskStatus)) {
					String failedReason = null;
					if (isNotEmpty(params.failedReason)) {
						failedReason = ScriptEvaluator.evalJq(params.failedReason, payload);
					}
					task.setReasonForIncompletion(failedReason);
				}

				// Create task update wrapper and update the task
				TaskResult taskResult = new TaskResult(task);
				executor.updateTask(taskResult);
				output.add(workflow.getWorkflowId());

			} catch (Exception ex) {
				String msg = String.format("Reference Keys Match failed for taskId=%s, messageId=%s, event=%s, workflowId=%s, correlationId=%s, payload=%s",
					task.getTaskId(), ee.getMessageId(), ee.getEvent(), task.getWorkflowInstanceId(), task.getCorrelationId(), payload);
				logger.warn(msg, ex);
			} finally {
				if (ndcCleanup) {
					NDC.remove();
				}
			}
		});

		return new ArrayList<>(output);
	}

	// generate all possible tags based on atlasId, versionId
	private Set<String> generateTags(String atlasId, String versionId) {
		Set<String> tags = new HashSet<>();
		if (StringUtils.isNotBlank(versionId)) {
			tags.addAll(Arrays.asList("featureVersionId" + versionId, "episodeVersionId" + versionId));
		}
		tags.addAll(Arrays.asList("featureVersionId" + atlasId, "episodeVersionId" + atlasId));
		return tags;
	}

	private boolean matches(List<ReferenceKey> taskRefKeys, String atlasId, String versionId) {
		if (StringUtils.isNotBlank(versionId)) {
			return taskRefKeys.stream().anyMatch(trk -> Objects.equals(trk.titleKeys.featureVersionId, versionId) ||
					Objects.equals(trk.titleKeys.episodeVersionId, versionId));
		}

		return taskRefKeys.stream().anyMatch(trk -> Objects.equals(trk.titleKeys.featureVersionId, atlasId) ||
			Objects.equals(trk.titleKeys.featureId, atlasId) ||
			Objects.equals(trk.titleKeys.episodeVersionId, atlasId) ||
			Objects.equals(trk.titleKeys.seriesId, atlasId) ||
			Objects.equals(trk.titleKeys.seasonId, atlasId) ||
			Objects.equals(trk.titleKeys.episodeId, atlasId));
	}

	private static class ActionParams {
		public String status;
		public String failedReason;
		public String expression;
		public Set<String> taskRefNames;
		public Map<String, String> statuses;
		public String atlasId;
		public String versionId;
	}

	private static class ReferenceKey {
		public TitleKeys titleKeys;

		@Override
		public String toString() {
			return "{" +
				"titleKeys=" + titleKeys + '}';
		}
	}

	private static class TitleKeys {
		public String featureId;
		public String featureVersionId;

		public String seriesId;
		public String seasonId;
		public String episodeId;
		public String episodeVersionId;

		@Override
		public String toString() {
			return "TitleKeys{" +
				"featureId='" + featureId + '\'' +
				", featureVersionId='" + featureVersionId + '\'' +
				", seriesId='" + seriesId + '\'' +
				", seasonId='" + seasonId + '\'' +
				", episodeId='" + episodeId + '\'' +
				", episodeVersionId='" + episodeVersionId + '\'' +
				'}';
		}
	}
}

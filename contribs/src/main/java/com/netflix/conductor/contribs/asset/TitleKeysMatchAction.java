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

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNoneEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@Singleton
public class TitleKeysMatchAction implements JavaEventAction {
	private static Logger logger = LoggerFactory.getLogger(ReferenceKeysMatchAction.class);
	private final WorkflowExecutor executor;
	private final ObjectMapper mapper;

	@Inject
	public TitleKeysMatchAction(WorkflowExecutor executor, ObjectMapper mapper) {
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

		Map<String, Object> titleKeysMap = ScriptEvaluator.evaluateMap(params.titleKeys, payload);
		Map<String, Object> titleVersionMap = ScriptEvaluator.evaluateMap(params.titleVersion, payload);

		ReferenceKey eventRefKeys = new ReferenceKey();
		eventRefKeys.titleKeys = mapper.convertValue(titleKeysMap, TitleKeys.class);
		eventRefKeys.titleVersion = mapper.convertValue(titleVersionMap, TitleVersion.class);

		// Lets find WAIT + IN_PROGRESS tasks directly via edao
		String ndcValue = NDC.peek();
		boolean taskNamesDefined = CollectionUtils.isNotEmpty(params.taskRefNames);
		List<Task> tasks = executor.getPendingSystemTasks(Wait.NAME);
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
				if (!matches(taskRefKeys, eventRefKeys)) {
					return;
				}

				//Otherwise update the task as we found it
				task.setStatus(taskStatus);
				task.getOutputData().put("conductor.event.name", ee.getEvent());
				task.getOutputData().put("conductor.event.payload", payload);
				task.getOutputData().put("conductor.event.messageId", ee.getMessageId());
				logger.debug("Updating task " + task + ". workflowId=" + workflow.getWorkflowId()
					+ ",correlationId=" + workflow.getCorrelationId()
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

	private boolean matches(List<ReferenceKey> taskRefKeys, ReferenceKey erk) {
		return taskRefKeys.stream().anyMatch(trk -> {
			/* Feature match logic:
				if .task.supplementalSubType != null and .task.supplementalSubType != ""
					and .task.type != null and .task.type != ""
					and .task.supplementalSubType == .event.supplementalSubType and .task.type == .event.type
					and .task.featureId != null and .task.featureId == .event.featureId
				then true
				elif ((.task.supplementalSubType == null or .task.supplementalSubType == "")
					  and
					  (.task.type == null or .task.type == "")
					 )
					 and .task.featureVersionId != null and .task.featureVersionId == .event.featureVersionId
					 and .task.featureId != null and .task.featureId == .event.featureId
				then true
			*/

			/* Episodic match logic
                if .task.supplementalSubType != null and .task.supplementalSubType != ""
                    and .task.type != null and .task.type != ""
                    and .task.supplementalSubType == .event.supplementalSubType
                    and .task.type == .event.type
                    and .task.episodeId != null and .task.episodeId == .event.episodeId
                    and .task.seasonId != null and .task.seasonId == .event.seasonId
                    and .task.seriesId != null and .task.seriesId == .event.seriesId
                then true
                elif ((.task.supplementalSubType == null or .task.supplementalSubType == "")
                       and
                       (.task.type == null or .task.type == "")
                      )
                    and .task.episodeVersionId != null and .task.episodeVersionId == .event.episodeVersionId
                    and .task.episodeId != null and .task.episodeId == .event.episodeId
                    and .task.seasonId != null and .task.seasonId == .event.seasonId
                    and .task.seriesId != null and .task.seriesId == .event.seriesId
                then true
			 */

				/* Series match logic:
				if .task.supplementalSubType != null and .task.supplementalSubType != ""
					and .task.type != null and .task.type != ""
					and .task.supplementalSubType == .event.supplementalSubType and .task.type == .event.type
					and .task.seriesId != null and .task.seriesId == .event.seriesId
				then true
				elif ((.task.supplementalSubType == null or .task.supplementalSubType == "")
					  and
					  (.task.type == null or .task.type == "")
					 )
					 and .task.seriesVersionId != null and .task.seriesVersionId == .event.seriesVersionId
					 and .task.seriesId != null and .task.seriesId == .event.seriesId
				then true
			*/

					/* Franchise match logic:
				if .task.supplementalSubType != null and .task.supplementalSubType != ""
					and .task.type != null and .task.type != ""
					and .task.supplementalSubType == .event.supplementalSubType and .task.type == .event.type
					and .task.franchiseId != null and .task.franchiseId == .event.franchiseId
				then true
				elif ((.task.supplementalSubType == null or .task.supplementalSubType == "")
					  and
					  (.task.type == null or .task.type == "")
					 )
					 and .task.franchiseVersionId != null and .task.franchiseVersionId == .event.franchiseVersionId
					 and .task.franchiseId != null and .task.franchiseId == .event.franchiseId
				then true
			*/

						/* Season match logic:
				if .task.supplementalSubType != null and .task.supplementalSubType != ""
					and .task.type != null and .task.type != ""
					and .task.supplementalSubType == .event.supplementalSubType and .task.type == .event.type
					and .task.seasonId != null and .task.seasonId == .event.seasonId
				    and .task.seriesId != null and .task.seriesId == .event.seriesId
				then true
				elif ((.task.supplementalSubType == null or .task.supplementalSubType == "")
					  and
					  (.task.type == null or .task.type == "")
					 )
					 and .task.seasonVersionId != null and .task.seasonVersionId == .event.seasonVersionId
					 and .task.seasonId != null and .task.seasonId == .event.seasonId
					 and .task.seriesId != null and .task.seriesId == .event.seriesId
				then true
			*/

			boolean isFeature = nonNull(trk.titleKeys) && isNotEmpty(trk.titleKeys.featureId);
			boolean isEpisodic = nonNull(trk.titleKeys) && isNotEmpty(trk.titleKeys.episodeId);
			boolean isFranchise = nonNull(trk.titleKeys) && isNoneEmpty(trk.titleKeys.franchiseId, trk.titleKeys.franchiseVersionId);
			boolean isSeries = nonNull(trk.titleKeys) && isNoneEmpty(trk.titleKeys.seriesId, trk.titleKeys.seriesVersionId);
			boolean isSeason = nonNull(trk.titleKeys) && isNoneEmpty(trk.titleKeys.seasonId, trk.titleKeys.seasonVersionId);
			boolean isSupplemental = nonNull(trk.titleVersion) && isNoneEmpty(trk.titleVersion.supplementalSubType, trk.titleVersion.type);

			if (isFeature && isSupplemental) {
				return (isNotEmpty(trk.titleKeys.featureId) &&
					Objects.equals(trk.titleVersion.supplementalSubType, erk.titleVersion.supplementalSubType) &&
					Objects.equals(trk.titleKeys.featureId, erk.titleKeys.featureId) &&
					Objects.equals(trk.titleVersion.type, erk.titleVersion.type)) || (isNoneEmpty(trk.titleKeys.featureId, trk.titleKeys.featureVersionId) &&
					Objects.equals(trk.titleKeys.featureVersionId, erk.titleKeys.featureVersionId) &&
					Objects.equals(trk.titleKeys.featureId, erk.titleKeys.featureId));

			} else if (isEpisodic && isSupplemental) {
				return (isNoneEmpty(trk.titleKeys.episodeId, trk.titleKeys.seasonId, trk.titleKeys.seriesId) &&
					Objects.equals(trk.titleVersion.supplementalSubType, erk.titleVersion.supplementalSubType) &&
					Objects.equals(trk.titleVersion.type, erk.titleVersion.type) &&
					Objects.equals(trk.titleKeys.episodeId, erk.titleKeys.episodeId) &&
					Objects.equals(trk.titleKeys.seasonId, erk.titleKeys.seasonId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId)) || (isNoneEmpty(trk.titleKeys.episodeId, trk.titleKeys.seasonId, trk.titleKeys.seriesId, trk.titleKeys.episodeVersionId) &&
					Objects.equals(trk.titleKeys.episodeVersionId, erk.titleKeys.episodeVersionId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId) &&
					Objects.equals(trk.titleKeys.seasonId, erk.titleKeys.seasonId) &&
					Objects.equals(trk.titleKeys.episodeId, erk.titleKeys.episodeId));

			} else if (isSeries && isSupplemental) {
				return (isNotEmpty(trk.titleKeys.seriesId) &&
					Objects.equals(trk.titleVersion.supplementalSubType, erk.titleVersion.supplementalSubType) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId) &&
					Objects.equals(trk.titleVersion.type, erk.titleVersion.type)) || (isNoneEmpty(trk.titleKeys.seriesId, trk.titleKeys.seriesVersionId) &&
					Objects.equals(trk.titleKeys.seriesVersionId, erk.titleKeys.seriesVersionId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId));

			} else if (isFranchise && isSupplemental) {
				return (isNotEmpty(trk.titleKeys.franchiseId) &&
					Objects.equals(trk.titleVersion.supplementalSubType, erk.titleVersion.supplementalSubType) &&
					Objects.equals(trk.titleKeys.franchiseId, erk.titleKeys.franchiseId) &&
					Objects.equals(trk.titleVersion.type, erk.titleVersion.type)) || (isNoneEmpty(trk.titleKeys.franchiseId, trk.titleKeys.franchiseVersionId) &&
					Objects.equals(trk.titleKeys.franchiseVersionId, erk.titleKeys.franchiseVersionId) &&
					Objects.equals(trk.titleKeys.franchiseId, erk.titleKeys.franchiseId));

			} else if (isSeason && isSupplemental) {
				return (isNoneEmpty(trk.titleKeys.seasonId, trk.titleKeys.seriesId) &&
					Objects.equals(trk.titleVersion.supplementalSubType, erk.titleVersion.supplementalSubType) &&
					Objects.equals(trk.titleKeys.seasonId, erk.titleKeys.seasonId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId) &&
					Objects.equals(trk.titleVersion.type, erk.titleVersion.type)) || (isNoneEmpty(trk.titleKeys.seasonId, trk.titleKeys.seriesId, trk.titleKeys.seasonVersionId) &&
					Objects.equals(trk.titleKeys.seasonVersionId, erk.titleKeys.seasonVersionId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId) &&
					Objects.equals(trk.titleKeys.seasonId, erk.titleKeys.seasonId));

			} else if (isFeature) {
				return isNoneEmpty(trk.titleKeys.featureId, trk.titleKeys.featureVersionId) &&
					Objects.equals(trk.titleKeys.featureVersionId, erk.titleKeys.featureVersionId) &&
					Objects.equals(trk.titleKeys.featureId, erk.titleKeys.featureId);
			} else if (isEpisodic) {
				return isNoneEmpty(trk.titleKeys.episodeId, trk.titleKeys.seasonId, trk.titleKeys.seriesId, trk.titleKeys.episodeVersionId) &&
					Objects.equals(trk.titleKeys.episodeVersionId, erk.titleKeys.episodeVersionId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId) &&
					Objects.equals(trk.titleKeys.seasonId, erk.titleKeys.seasonId) &&
					Objects.equals(trk.titleKeys.episodeId, erk.titleKeys.episodeId);
			} else if (isSeries) {
				return isNoneEmpty(trk.titleKeys.seriesId, trk.titleKeys.seriesVersionId) &&
					Objects.equals(trk.titleKeys.seriesVersionId, erk.titleKeys.seriesVersionId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId);
			} else if (isFranchise) {
				return isNoneEmpty(trk.titleKeys.franchiseId, trk.titleKeys.franchiseVersionId) &&
					Objects.equals(trk.titleKeys.franchiseVersionId, erk.titleKeys.franchiseVersionId) &&
					Objects.equals(trk.titleKeys.franchiseId, erk.titleKeys.franchiseId);
			} else if (isSeason) {
				return isNoneEmpty(trk.titleKeys.seasonId, trk.titleKeys.seriesId, trk.titleKeys.seasonVersionId) &&
					Objects.equals(trk.titleKeys.seasonVersionId, erk.titleKeys.seasonVersionId) &&
					Objects.equals(trk.titleKeys.seriesId, erk.titleKeys.seriesId) &&
					Objects.equals(trk.titleKeys.seasonId, erk.titleKeys.seasonId);
			}
			return false;
		});
	}

	private static class ActionParams {
		public String status;
		public String failedReason;
		public String expression;
		public Set<String> taskRefNames;
		public Map<String, String> statuses;
		public Map<String, String> titleKeys;
		public Map<String, String> titleVersion;
	}

	private static class ReferenceKey {
		public TitleKeys titleKeys;
		public TitleVersion titleVersion;

		@Override
		public String toString() {
			return "{" +
				"titleKeys=" + titleKeys +
				", titleVersion=" + titleVersion +
				'}';
		}
	}

	private static class TitleKeys {
		public String featureId;
		public String featureVersionId;

		public String seriesId;
		public String seasonId;
		public String episodeId;
		public String episodeVersionId;

		public String franchiseId;
		public String seriesVersionId;
		public String seasonVersionId;
		public String franchiseVersionId;

		@Override
		public String toString() {
			return "TitleKeys{" +
				"featureId='" + featureId + '\'' +
				", featureVersionId='" + featureVersionId + '\'' +
				", seriesId='" + seriesId + '\'' +
				", seasonId='" + seasonId + '\'' +
				", episodeId='" + episodeId + '\'' +
				", episodeVersionId='" + episodeVersionId + '\'' +
				", franchiseId='" + franchiseId + '\'' +
				", franchiseVersionId='" + franchiseVersionId + '\'' +
				", seriesVersionId='" + seriesVersionId + '\'' +
				", seasonVersionId='" + seasonVersionId + '\'' +
				'}';
		}
	}

	private static class TitleVersion {
		public String type;
		public String supplementalSubType;

		@Override
		public String toString() {
			return "TitleVersion{" +
				"type='" + type + '\'' +
				", supplementalSubType='" + supplementalSubType + '\'' +
				'}';
		}
	}
}

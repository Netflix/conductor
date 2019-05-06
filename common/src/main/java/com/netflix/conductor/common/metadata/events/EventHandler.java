/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.common.metadata.events;

import java.util.*;

/**
 * @author Viren
 * Defines an event handler
 */
public class EventHandler {

	private String name;

	private String event;

	private String condition;

	private List<Action> actions = new LinkedList<>();

	private boolean active;

	private String tags;

	private boolean retryEnabled;

	public EventHandler() {

	}

	/**
	 * @return the name MUST be unique within a conductor instance
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the event
	 */
	public String getEvent() {
		return event;
	}

	/**
	 * @param event the event to set
	 */
	public void setEvent(String event) {
		this.event = event;
	}

	/**
	 * @return the condition
	 */
	public String getCondition() {
		return condition;
	}

	/**
	 * @param condition the condition to set
	 */
	public void setCondition(String condition) {
		this.condition = condition;
	}

	/**
	 * @return the actions
	 */
	public List<Action> getActions() {
		return actions;
	}

	/**
	 * @param actions the actions to set
	 */
	public void setActions(List<Action> actions) {
		this.actions = actions;
	}

	/**
	 * @return the active
	 */
	public boolean isActive() {
		return active;
	}

	/**
	 * @param active if set to false, the event handler is deactivated
	 */
	public void setActive(boolean active) {
		this.active = active;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public boolean isRetryEnabled() {
		return retryEnabled;
	}

	public void setRetryEnabled(boolean retryEnabled) {
		this.retryEnabled = retryEnabled;
	}

	public static class Action {

		public enum Type {start_workflow, complete_task, fail_task, update_task, find_update, java_action}

		private Type action;

		private String condition;

		private StartWorkflow start_workflow;

		private TaskDetails complete_task;

		private TaskDetails fail_task;

		private UpdateTask update_task;

		private FindUpdate find_update;

		private boolean expandInlineJSON;

		private JavaAction java_action;

		/**
		 * @return the action
		 */
		public Type getAction() {
			return action;
		}

		/**
		 * @param action the action to set
		 */
		public void setAction(Type action) {
			this.action = action;
		}

		/**
		 * @return the condition
		 */
		public String getCondition() {
			return condition;
		}

		/**
		 * @param condition the condition to set
		 */
		public void setCondition(String condition) {
			this.condition = condition;
		}

		/**
		 * @return the start_workflow
		 */
		public StartWorkflow getStart_workflow() {
			return start_workflow;
		}

		/**
		 * @param start_workflow the start_workflow to set
		 */
		public void setStart_workflow(StartWorkflow start_workflow) {
			this.start_workflow = start_workflow;
		}

		/**
		 * @return the complete_task
		 */
		public TaskDetails getComplete_task() {
			return complete_task;
		}

		/**
		 * @param complete_task the complete_task to set
		 */
		public void setComplete_task(TaskDetails complete_task) {
			this.complete_task = complete_task;
		}

		/**
		 * @return the fail_task
		 */
		public TaskDetails getFail_task() {
			return fail_task;
		}

		/**
		 * @param fail_task the fail_task to set
		 */
		public void setFail_task(TaskDetails fail_task) {
			this.fail_task = fail_task;
		}

		/**
		 * @return the progress task object
		 */
		public UpdateTask getUpdate_task() {
			return update_task;
		}

		/**
		 * @param update_task the progress task object to set
		 */
		public void setUpdate_task(UpdateTask update_task) {
			this.update_task = update_task;
		}

		public FindUpdate getFind_update() {
			return find_update;
		}

		public void setFind_update(FindUpdate find_update) {
			this.find_update = find_update;
		}

		/**
		 * @param expandInlineJSON when set to true, the in-lined JSON strings are expanded to a full json document
		 */
		public void setExpandInlineJSON(boolean expandInlineJSON) {
			this.expandInlineJSON = expandInlineJSON;
		}

		/**
		 * @return true if the json strings within the payload should be expanded.
		 */
		public boolean isExpandInlineJSON() {
			return expandInlineJSON;
		}

		public JavaAction getJava_action() {
			return java_action;
		}

		public void setJava_action(JavaAction java_action) {
			this.java_action = java_action;
		}

		@Override
		public String toString() {
			return "{action=" + action +
					", condition='" + condition + '\'' +
					(start_workflow != null ? ", start_workflow=" + start_workflow : "") +
					(complete_task != null ? ", complete_task=" + complete_task : "") +
					(fail_task != null ? ", fail_task=" + fail_task : "") +
					(update_task != null ? ", update_task=" + update_task : "") +
					(find_update != null ? ", find_update=" + find_update : "") +
					(java_action != null ? ", java_action=" + java_action : "") +
					", expandInlineJSON=" + expandInlineJSON + '}';
		}
	}

	public static class TaskDetails {

		private String workflowId;

		private String taskRefName;

		private Map<String, Object> output = new HashMap<>();

		/**
		 * @return the workflowId
		 */
		public String getWorkflowId() {
			return workflowId;
		}

		/**
		 * @param workflowId the workflowId to set
		 */
		public void setWorkflowId(String workflowId) {
			this.workflowId = workflowId;
		}

		/**
		 * @return the taskRefName
		 */
		public String getTaskRefName() {
			return taskRefName;
		}

		/**
		 * @param taskRefName the taskRefName to set
		 */
		public void setTaskRefName(String taskRefName) {
			this.taskRefName = taskRefName;
		}

		/**
		 * @return the output
		 */
		public Map<String, Object> getOutput() {
			return output;
		}

		/**
		 * @param output the output to set
		 */
		public void setOutput(Map<String, Object> output) {
			this.output = output;
		}


		@Override
		public String toString() {
			return "TaskDetails{" +
					"workflowId='" + workflowId + '\'' +
					", taskRefName='" + taskRefName + '\'' +
					", output=" + output +
					'}';
		}
	}

	public static class StartWorkflow {

		private String name;

		private Integer version;

		private String correlationId;

		private Map<String, Object> input = new HashMap<>();

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the version
		 */
		public Integer getVersion() {
			return version;
		}

		/**
		 * @param version the version to set
		 */
		public void setVersion(Integer version) {
			this.version = version;
		}


		/**
		 * @return the correlationId
		 */
		public String getCorrelationId() {
			return correlationId;
		}

		/**
		 * @param correlationId the correlationId to set
		 */
		public void setCorrelationId(String correlationId) {
			this.correlationId = correlationId;
		}

		/**
		 * @return the input
		 */
		public Map<String, Object> getInput() {
			return input;
		}

		/**
		 * @param input the input to set
		 */
		public void setInput(Map<String, Object> input) {
			this.input = input;
		}

		@Override
		public String toString() {
			return "StartWorkflow{" +
					"name='" + name + '\'' +
					", version=" + version +
					", correlationId='" + correlationId + '\'' +
					", input=" + input +
					'}';
		}
	}

	public static class UpdateTask {

		private String workflowId;

		private String taskId;

		private String taskRef;

		private String status;

		private String failedReason;

		private boolean resetStartTime;

		private Map<String, String> statuses = new HashMap<>();

		private Map<String, Object> output = new HashMap<>();

		public String getWorkflowId() {
			return workflowId;
		}

		public void setWorkflowId(String workflowId) {
			this.workflowId = workflowId;
		}

		public Map<String, Object> getOutput() {
			return output;
		}

		public void setOutput(Map<String, Object> output) {
			this.output = output;
		}

		public String getTaskId() {
			return taskId;
		}

		public void setTaskId(String taskId) {
			this.taskId = taskId;
		}

		public String getTaskRef() {
			return taskRef;
		}

		public void setTaskRef(String taskRef) {
			this.taskRef = taskRef;
		}

		public boolean getResetStartTime() {
			return resetStartTime;
		}

		public void setResetStartTime(boolean resetStartTime) {
			this.resetStartTime = resetStartTime;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public Map<String, String> getStatuses() {
			return statuses;
		}

		public void setStatuses(Map<String, String> statuses) {
			this.statuses = statuses;
		}

		public String getFailedReason() {
			return failedReason;
		}

		public void setFailedReason(String failedReason) {
			this.failedReason = failedReason;
		}

		@Override
		public String toString() {
			return "UpdateTask{" +
					"workflowId='" + workflowId + '\'' +
					", taskId='" + taskId + '\'' +
					", taskRef='" + taskRef + '\'' +
					", status='" + status + '\'' +
					", resetStartTime=" + resetStartTime +
					", failedReason='" + failedReason + '\'' +
					", statuses=" + statuses +
					", output=" + output +
					'}';
		}
	}

	public static class FindUpdate {
		private String status;
		private String failedReason;
		private String expression;
		private Set<String> taskRefNames = new HashSet<>();
		private Map<String, String> statuses = new HashMap<>();
		private Map<String, String> inputParameters = new HashMap<>();

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public String getFailedReason() {
			return failedReason;
		}

		public String getExpression() {
			return expression;
		}

		public void setExpression(String expression) {
			this.expression = expression;
		}

		public void setFailedReason(String failedReason) {
			this.failedReason = failedReason;
		}

		public Map<String, String> getStatuses() {
			return statuses;
		}

		public void setStatuses(Map<String, String> statuses) {
			this.statuses = statuses;
		}

		public Map<String, String> getInputParameters() {
			return inputParameters;
		}

		public void setInputParameters(Map<String, String> inputParameters) {
			this.inputParameters = inputParameters;
		}

		public Set<String> getTaskRefNames() {
			return taskRefNames;
		}

		public void setTaskRefNames(Set<String> taskRefNames) {
			this.taskRefNames = taskRefNames;
		}

		@Override
		public String toString() {
			return "FindUpdate{" +
					"taskRefNames=" + taskRefNames +
					", status='" + status + '\'' +
					", statuses=" + statuses +
					", expression='" + expression + '\'' +
					", failedReason='" + failedReason + '\'' +
					", inputParameters='" + inputParameters + '\'' +
					'}';
		}
	}

	public static class JavaAction {
		private String className;
		private Map<String, Object> inputParameters;

		public String getClassName() {
			return className;
		}

		public void setClassName(String className) {
			this.className = className;
		}

		public Map<String, Object> getInputParameters() {
			return inputParameters;
		}

		public void setInputParameters(Map<String, Object> inputParameters) {
			this.inputParameters = inputParameters;
		}

		@Override
		public String toString() {
			return "JavaAction{" +
					"className='" + className + '\'' +
					", inputParameters=" + inputParameters +
					'}';
		}
	}
}

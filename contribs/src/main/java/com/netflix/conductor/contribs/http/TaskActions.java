package com.netflix.conductor.contribs.http;

import java.util.HashMap;
import java.util.Map;

public class TaskActions {
	private TaskAction preTask;
	private TaskAction postTask;

	public TaskAction getPreTask() {
		return preTask;
	}

	public void setPreTask(TaskAction preTask) {
		this.preTask = preTask;
	}

	public TaskAction getPostTask() {
		return postTask;
	}

	public void setPostTask(TaskAction postTask) {
		this.postTask = postTask;
	}

	@Override
	public String toString() {
		return "TaskActions{" +
				"preTask=" + preTask +
				", postTask=" + postTask +
				'}';
	}

	public static class TaskAction {
		private String sink;
		private Map<String, Object> inputParameters = new HashMap<>();

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

		@Override
		public String toString() {
			return "TaskAction{" +
					"sink='" + sink + '\'' +
					", inputParameters=" + inputParameters +
					'}';
		}
	}
}



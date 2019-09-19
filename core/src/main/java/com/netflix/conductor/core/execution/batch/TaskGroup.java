package com.netflix.conductor.core.execution.batch;

import com.netflix.conductor.common.metadata.tasks.Task;

import java.util.List;

public class TaskGroup {
	private String key;
	private List<Task> tasks;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public List<Task> getTasks() {
		return tasks;
	}

	public void setTasks(List<Task> tasks) {
		this.tasks = tasks;
	}

	@Override
	public String toString() {
		return "TaskGroup{" +
			"key='" + key + '\'' +
			", tasks=" + tasks +
			'}';
	}
}

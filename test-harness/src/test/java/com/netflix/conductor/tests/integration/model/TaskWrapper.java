package com.netflix.conductor.tests.integration.model;

import java.util.List;

import com.netflix.conductor.common.metadata.tasks.TaskDef;

public class TaskWrapper {

	private List<TaskDef> taskDefs;

	public List<TaskDef> getTaskDefs() {
		return taskDefs;
	}

	public void setTaskDefs(List<TaskDef> taskDefs) {
		this.taskDefs = taskDefs;
	}

	@Override
	public String toString() {
		return "TaskWrapper{" + "taskDefs=" + taskDefs + '}';
	}
}
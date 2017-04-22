/**
 * 
 */
package com.netflix.conductor.tests.utils;

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;

/**
 * @author Viren
 *
 */
public class UserTask extends WorkflowSystemTask {

	public UserTask() {
		super("USER_TASK");
	}
	
	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
		task.setStatus(Status.COMPLETED);
	}
	
	@Override
	public boolean isAsync() {
		return true;
	}
}

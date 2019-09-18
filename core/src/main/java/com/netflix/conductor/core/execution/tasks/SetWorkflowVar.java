package com.netflix.conductor.core.execution.tasks;

import java.util.Map;

import javax.inject.Inject;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

public class SetWorkflowVar extends WorkflowSystemTask {
	public static final String NAME = "SET_WORKFLOW_VAR";
	public SetWorkflowVar() {
		super(NAME);
	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) {
		updateVariables(task.getInputData(), workflow);
		task.setStatus(Status.COMPLETED);
		return true;
	}
	
	private boolean updateVariables( Map<String, Object> input, Workflow workflow) {
    	boolean doUpdate = false;
    	Map<String, Object> workflowVars = workflow.getVariables();
    	if (input != null && input.size() > 0) {
    		for (String key : input.keySet()) {
    			if (workflowVars.containsKey(key)) {
    				workflowVars.put(key, input.get(key));
    				doUpdate = true;
    			}
    		}
		}
    	return doUpdate;
    }

}

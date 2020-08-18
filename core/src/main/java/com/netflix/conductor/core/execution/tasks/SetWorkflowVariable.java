package com.netflix.conductor.core.execution.tasks;

import java.util.Map;

import javax.inject.Inject;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

public class SetWorkflowVariable extends WorkflowSystemTask {

    public static final String NAME = "SET_WORKFLOW_VARIABLE";
    public SetWorkflowVariable() {
        super(NAME);
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor provider) {
        Map<String, Object> workflowVars = workflow.getVariables();
        Map<String, Object> input = task.getInputData();
        if (input != null && input.size() > 0) {
            input.keySet().forEach(key -> workflowVars.put(key, input.get(key)));
        }
        task.setStatus(Task.Status.COMPLETED);
    }
}

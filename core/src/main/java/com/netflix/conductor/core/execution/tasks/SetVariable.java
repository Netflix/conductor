package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetVariable extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(SetVariable.class);

    public SetVariable() {
        super("SET_VARIABLE");
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
        try {
            Map<String, Object> variables = workflow.getVariables();
            Map<String, Object> input = task.getInputData();
            String taskId = task.getTaskId();
            List<String> newKeys;
            Map<String, Object> previousValues;

            if (input != null && input.size() > 0) {
                newKeys = new ArrayList<>();
                previousValues = new HashMap<>();
                input.keySet()
                        .forEach(
                                key -> {
                                    if (variables.containsKey(key)) {
                                        previousValues.put(key, variables.get(key));
                                    } else {
                                        newKeys.add(key);
                                    }
                                    variables.put(key, input.get(key));
                                    logger.debug(
                                            "Task: {} setting value for variable: {}", taskId, key);
                                });
            }

            task.setStatus(Task.Status.COMPLETED);

        } catch (Exception e) {
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
            logger.debug(e.getMessage(), e);
        }
    }
}

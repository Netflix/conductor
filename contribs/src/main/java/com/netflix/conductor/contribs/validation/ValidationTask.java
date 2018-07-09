/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.contribs.validation;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
public class ValidationTask extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(ValidationTask.class);
    private static final String REASON_PARAMETER = "reason";
    private static final String PAYLOAD_PARAMETER = "payload";
    private static final String CONDITIONS_PARAMETER = "conditions";

    public ValidationTask() {
        super("VALIDATION");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        Map<String, Object> taskInput = task.getInputData();
        Map<String, Object> taskOutput = task.getOutputData();

        Object payloadObj = taskInput.get(PAYLOAD_PARAMETER);
        logger.debug("Payload object is " + payloadObj);
        if(payloadObj == null) {
            task.setReasonForIncompletion("Missing '" + PAYLOAD_PARAMETER + "' in input parameters");
            task.setStatus(Task.Status.FAILED);
            return;
        }

        Map<String, String> conditionsObj = (Map<String, String>)taskInput.get(CONDITIONS_PARAMETER);
        logger.debug("Conditions object is " + conditionsObj);
        if (conditionsObj == null){
            task.setReasonForIncompletion("Missing '" + CONDITIONS_PARAMETER + "' in input parameters");
            task.setStatus(Task.Status.FAILED);
            return;
        } else if (conditionsObj.isEmpty()) {
            task.setReasonForIncompletion("'" + CONDITIONS_PARAMETER + "' input parameter is empty");
            task.setStatus(Task.Status.FAILED);
            return;
        }

        // Set the task status to complete at the begin
        task.setStatus(Status.COMPLETED);

        // Default is true. Will be set to false upon some condition fails
        AtomicBoolean overallStatus = new AtomicBoolean(true);

        // Go over all conditions and evaluate them
        conditionsObj.forEach((name, condition) -> {
            try {
                Boolean success = ScriptEvaluator.evalBool(condition, payloadObj);
                logger.debug("Evaluation resulted in " + success + " for " + name + "=" + condition);

                // Add condition evaluation result into output map
                addEvalResult(task, name, success);

                // Failed ?
                if (!success) {
                    // Set the over all status to false
                    overallStatus.set(false);
                }
            } catch (Exception ex) {
                logger.error("Evaluation failed for " + name + "=" + condition, ex);

                // Set the error message instead of false
                addEvalResult(task, name, ex.getMessage());

                // Set the over all status to false
                overallStatus.set(false);
            }
        });

        // Set the overall status to the output map
        taskOutput.put("overallStatus", overallStatus.get());

        // Get an additional configuration
        boolean failOnFalse = getFailOnFalse(task);

        // Build the overall reason
        String overallReason = (String)taskInput.get(REASON_PARAMETER);
        if (overallReason == null) {
            overallReason = "Payload validation failed";
        }
        // Set the overall reason to the output map
        taskOutput.put("overallReason", overallReason);

        // If overall status is false and we need to fail whole workflow
        if (!overallStatus.get() && failOnFalse) {
            task.setReasonForIncompletion(overallReason);
            task.setStatus(Status.FAILED);
        }
    }

    @SuppressWarnings("unchecked")
    private void addEvalResult(Task task, String condition, Object result) {
        Map<String, Object> taskOutput = task.getOutputData();
        Map<String, Object> conditions = (Map<String, Object>)taskOutput.get(CONDITIONS_PARAMETER);
        if (conditions == null) {
            conditions = new HashMap<>();
            taskOutput.put(CONDITIONS_PARAMETER, conditions);
        }
        conditions.put(condition, result);
    }

    private boolean getFailOnFalse(Task task) {
        Object obj = task.getInputData().get("failOnFalse");
        if (obj instanceof Boolean) {
            return (Boolean)obj;
        } else if (obj instanceof String) {
            return Boolean.parseBoolean((String)obj);
        }
        return true;
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        return false;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        task.setStatus(Status.CANCELED);
    }
}

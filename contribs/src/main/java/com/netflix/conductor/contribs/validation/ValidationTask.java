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
import java.util.Map;

/**
 * @author Viren
 *
 */
@Singleton
public class ValidationTask extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(ValidationTask.class);
    private static final String PAYLOAD_PARAMETER = "payload";
    private static final String CONDITIONS_PARAMETER = "conditions";
    private static final String NAME = "VALIDATION";

    public ValidationTask() {
        super(NAME);
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

        // Go over all conditions and evaluate them
        conditionsObj.forEach((name, condition) -> {
            try {
                Boolean success = ScriptEvaluator.evalBool(condition, payloadObj);
                logger.debug("Evaluation resulted in " + success + " for " + name + "=" + condition);
                taskOutput.put(name, success);

                // Set the overall task status to FAILED if any of the conditions fails
                if (!success)
                    task.setStatus(Status.FAILED);
            } catch (Throwable ex) {
                // Set the overall task status to FAILED if any of the conditions fails
                task.setStatus(Status.FAILED);
                taskOutput.put(name, ex.getMessage());
                logger.error("Evaluation failed for " + name + "=" + condition, ex);
            }
        });
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

/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * @author X-Ultra
 * Task that enables execute Lambda script at workflow execution, For example,
 * <pre>
...
{
    "tasks": [
        {
            "name": "LAMBDA",
            "taskReferenceName": "lambda_test",
            "type": "LAMBDA",
            "inputParameters": {
               "input": "${workflow.input}",
                "scriptExpression": "if ($.input.a==1){return {testvalue: true}} else{return {testvalue: false} }"
            }
        }
    ]
}
...
 * </pre>
 * then to use task output, e.g.  <code>script_test.output.testvalue</code>
 */
public class Lambda extends WorkflowSystemTask {

    private static final Logger logger = LoggerFactory.getLogger(Lambda.class);
    private static final String QUERY_EXPRESSION_PARAMETER = "scriptExpression";
    public static final String TASK_NAME = "LAMBDA";

    public Lambda() {
        super(TASK_NAME);
        logger.info(TASK_NAME + " task initialized...");
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
        Map<String, Object> taskInput = task.getInputData();
        Map<String, Object> taskOutput = task.getOutputData();

        String scriptExpression;

        try {
            scriptExpression = (String) taskInput.get(QUERY_EXPRESSION_PARAMETER);
            if (StringUtils.isNotBlank(scriptExpression)) {
                String scriptExpressionBuilder = "function scriptFun(){" +
                        scriptExpression +
                        "} scriptFun();";

                logger.debug("scriptExpressionBuilder: {}, task: {}" , scriptExpressionBuilder, task.getTaskId());
                Object returnValue = ScriptEvaluator.eval(scriptExpressionBuilder, taskInput);
                taskOutput.put("result", returnValue);
                task.setStatus(Task.Status.COMPLETED);
            }
            else {
                logger.error("Empty {} in Lambda task. ", QUERY_EXPRESSION_PARAMETER);
                task.setReasonForIncompletion("Empty '" + QUERY_EXPRESSION_PARAMETER + "' in Lambda task's input parameters. A non-empty String value must be provided.");
                task.setStatus(Task.Status.FAILED);
            }
        } catch (Exception e) {
            logger.error("Failed to execute Lambda Task: {} in workflow: {}", task.getTaskId(), workflow.getWorkflowId(), e);
            task.setStatus(Task.Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
            taskOutput.put("error", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        }

        return true;
    }
}

/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/** @author x-ultra */
public class TestLambda {

    private final Workflow workflow = new Workflow();
    private final WorkflowExecutor executor = mock(WorkflowExecutor.class);

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void start() {
        Lambda lambda = new Lambda();

        Map inputObj = new HashMap();
        inputObj.put("a", 1);

        // test for scriptExpression == null
        Task task = new Task();
        task.getInputData().put("input", inputObj);
        lambda.execute(workflow, task, executor);
        assertEquals(Task.Status.FAILED, task.getStatus());

        // test for normal
        task = new Task();
        task.getInputData().put("input", inputObj);
        task.getInputData().put("scriptExpression", "if ($.input.a==1){return 1}else{return 0 } ");
        lambda.execute(workflow, task, executor);
        assertEquals(Task.Status.COMPLETED, task.getStatus());
        assertEquals(task.getOutputData().toString(), "{result=1}");

        // test for scriptExpression ScriptException
        task = new Task();
        task.getInputData().put("input", inputObj);
        task.getInputData().put("scriptExpression", "if ($.a.size==1){return 1}else{return 0 } ");
        lambda.execute(workflow, task, executor);
        assertEquals(Task.Status.FAILED, task.getStatus());
    }
}

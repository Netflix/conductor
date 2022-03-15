/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.sdk;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.sdk.task.InputParam;
import com.netflix.conductor.sdk.task.OutputParam;
import com.netflix.conductor.sdk.task.WorkflowTask;
import com.netflix.conductor.sdk.testing.WorkflowTestRunner;
import com.netflix.conductor.sdk.workflow.def.ConductorWorkflow;
import com.netflix.conductor.sdk.workflow.def.ValidationError;
import com.netflix.conductor.sdk.workflow.def.WorkflowBuilder;
import com.netflix.conductor.sdk.workflow.def.tasks.*;
import com.netflix.conductor.sdk.workflow.executor.WorkflowExecutor;
import com.netflix.conductor.tests.TestWorkflowInput;

import static org.junit.Assert.*;

public class SDKTests {

    private static final String AUTHORIZATION_HEADER = "X-Authorization";

    private static WorkflowExecutor executor;

    private static WorkflowTestRunner runner;

    @BeforeClass
    public static void init() throws IOException {
        runner = new WorkflowTestRunner(8080, "3.5.3");
        runner.init("com.netflix.conductor.sdk");
        executor = runner.getWorkflowExecutor();
    }

    @AfterClass
    public static void cleanUp() {
        runner.shutdown();
    }

    @WorkflowTask("get_user_info")
    public @OutputParam("zipCode") String getZipCode(@InputParam("name") String userName) {
        return "95014";
    }

    @WorkflowTask("task2")
    public @OutputParam("greetings") String task2() {
        return "Hello World";
    }

    @WorkflowTask("task3")
    public @OutputParam("greetings") String task3() {
        return "Hello World-3";
    }

    private ConductorWorkflow<TestWorkflowInput> registerTestWorkflow() {
        InputStream script = getClass().getResourceAsStream("/script.js");
        SimpleTask getUserInfo = new SimpleTask("get_user_info", "get_user_info");
        getUserInfo.input("name", ConductorWorkflow.input.get("name"));

        SimpleTask sendToCupertino = new SimpleTask("task2", "cupertino");
        SimpleTask sendToNYC = new SimpleTask("task2", "nyc");

        int len = 4;
        Task<?>[][] parallelTasks = new Task[len][1];
        for (int i = 0; i < len; i++) {
            parallelTasks[i][0] = new SimpleTask("task2", "task_parallel_" + i);
        }

        WorkflowBuilder<TestWorkflowInput> builder = new WorkflowBuilder<>(executor);
        TestWorkflowInput defaultInput = new TestWorkflowInput();
        defaultInput.setName("defaultName");

        builder.name("sdk_workflow_example")
                .version(1)
                .ownerEmail("hello@example.com")
                .description("Example Workflow")
                .restartable(true)
                .variables(new MyWorkflowState())
                .timeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF, 100)
                .defaultInput(defaultInput)
                .add(new Javascript("js", script))
                .add(new ForkJoin("parallel", parallelTasks))
                .add(getUserInfo)
                .add(
                        new Switch("decide2", "${workflow.input.zipCode}")
                                .switchCase("95014", sendToCupertino)
                                .switchCase("10121", sendToNYC))
                // .add(new SubWorkflow("subflow", "sub_workflow_example", 5))
                .add(new SimpleTask("task2", "task222"));

        ConductorWorkflow<TestWorkflowInput> workflow = builder.build();
        boolean registered = workflow.registerWorkflow(true, true);
        assertTrue(registered);

        return workflow;
    }

    @Test
    public void test() throws ValidationError {
        TestWorkflowInput workflowInput = new TestWorkflowInput("viren", "10121", "CA");
        try {
            Workflow run = registerTestWorkflow().execute(workflowInput).get(10, TimeUnit.SECONDS);
            assertEquals(
                    run.getReasonForIncompletion(),
                    Workflow.WorkflowStatus.COMPLETED,
                    run.getStatus());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void test2() throws ExecutionException, InterruptedException {
        registerTestWorkflow();

        TestWorkflowInput input = new TestWorkflowInput("Viren", "10121", "US");

        ConductorWorkflow<TestWorkflowInput> conductorWorkflow =
                new ConductorWorkflow<TestWorkflowInput>(executor)
                        .from("sdk_workflow_example", null);

        CompletableFuture<Workflow> execution = conductorWorkflow.execute(input);
        try {
            execution.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}

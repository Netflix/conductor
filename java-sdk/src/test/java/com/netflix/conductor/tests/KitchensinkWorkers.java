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
package com.netflix.conductor.tests;

import java.util.Random;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.sdk.task.InputParam;
import com.netflix.conductor.sdk.task.OutputParam;
import com.netflix.conductor.sdk.task.WorkflowTask;

public class KitchensinkWorkers {

    @WorkflowTask("task1")
    public TaskResult task1(Task task) {
        task.setStatus(Task.Status.COMPLETED);
        return new TaskResult(task);
    }

    @WorkflowTask("task2")
    public @OutputParam("greetings") String task2(@InputParam("name") String name) {
        return "Hello, " + name;
    }

    @WorkflowTask("task3")
    public @OutputParam("luckyNumber") int task3() {
        return new Random().nextInt(43);
    }
}

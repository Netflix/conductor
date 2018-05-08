/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.client.sample;

import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.task.WorkflowTaskCoordinator;
import com.netflix.conductor.client.worker.Worker;

/**
 * @author Viren
 *
 */
public class Main {

    public static void main(String[] args) {

        TaskClient taskClient = new TaskClient();
        taskClient.setRootURI("http://localhost:8080/api/");        //Point this to the server API

        int threadCount = 2;            //number of threads used to execute workers.  To avoid starvation, should be same or more than number of workers

        Worker worker1 = new SampleWorker("task_1");
        Worker worker2 = new SampleWorker("task_5");

        //Create WorkflowTaskCoordinator
        WorkflowTaskCoordinator.Builder builder = new WorkflowTaskCoordinator.Builder();
        WorkflowTaskCoordinator coordinator = builder.withWorkers(worker1, worker2).withThreadCount(threadCount).withTaskClient(taskClient).build();

        //Start for polling and execution of the tasks
        coordinator.init();

    }

}

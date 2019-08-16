/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.conductor.dao;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;

import java.util.List;

/**
 *
 * @author Manan
 * DAO to send message to kafka.
 */
public interface KafkaProducerDAO {

    void send(String t, Object value);

    void produceWorkflow(Workflow workflow);

    void produceTask(Task task);

    void produceTaskExecutionLogs(List<TaskExecLog> taskExecLogs);

    void produceMessage(String queue, Message message);

    void produceEventExecution(EventExecution eventExecution);
}
/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.KafkaProducerDAO;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.elasticsearch.client.Client;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

/**
 * @author Manan
 */
@Trace
@Singleton
public class ElasticSearchKafkaDAOV5 extends ElasticSearchDAOV5 {

    private KafkaProducerDAO kafkaProducerDAO;

    @Inject
    public ElasticSearchKafkaDAOV5(Client elasticSearchClient, ElasticSearchConfiguration config,
                                   ObjectMapper objectMapper, KafkaProducerDAO kafkaProducerDAO) {
       super(elasticSearchClient, config, objectMapper);
        this.kafkaProducerDAO = kafkaProducerDAO;
    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        kafkaProducerDAO.produceWorkflow(workflow);
    }

    @Override
    public void indexTask(Task task) {
       kafkaProducerDAO.produceTask(task);
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        kafkaProducerDAO.produceTaskExecutionLogs(taskExecLogs);
    }

    @Override
    public void addMessage(String queue, Message message) {
        kafkaProducerDAO.produceMessage(queue, message);
    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
       kafkaProducerDAO.produceEventExecution(eventExecution);
    }
}

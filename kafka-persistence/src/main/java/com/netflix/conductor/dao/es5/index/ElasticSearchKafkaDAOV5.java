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
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.utils.DataUtils;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.elasticsearch.client.Client;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * @author Manan
 */
@Trace
@Singleton
public class ElasticSearchKafkaDAOV5 extends ElasticSearchDAOV5 {

    private ProducerDAO producerDAO;

    @Inject
    public ElasticSearchKafkaDAOV5(Client elasticSearchClient, ElasticSearchConfiguration config,
                                   ObjectMapper objectMapper, ProducerDAO producerDAO) {
       super(elasticSearchClient, config, objectMapper);
        this.producerDAO = producerDAO;
    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        WorkflowSummary summary = new WorkflowSummary(workflow);
        producerDAO.send(DataUtils.WORKFLOW_DOC_TYPE, summary);
    }

    @Override
    public void indexTask(Task task) {
        TaskSummary summary = new TaskSummary(task);
        producerDAO.send(DataUtils.TASK_DOC_TYPE, summary);
    }

}

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
package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestDAOV5;
import com.netflix.conductor.dao.kafka.index.constants.OperationTypeConstants;
import com.netflix.conductor.dao.kafka.index.producer.KafkaProducer;
import com.netflix.conductor.dao.kafka.index.constants.RecordTypeConstants;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.metrics.Monitors;
import org.elasticsearch.client.RestClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * @author Manan
 */
@Trace
@Singleton
public class KafkaDAO extends ElasticSearchRestDAOV5 {

    private ProducerDAO producerDAO;

    @Inject
    public KafkaDAO(KafkaProducer producer, RestClient lowLevelRestClient, ElasticSearchConfiguration config, ObjectMapper objectMapper) {
        super(lowLevelRestClient, config, objectMapper);
        this.producerDAO = producer;
    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        WorkflowSummary summary = new WorkflowSummary(workflow);
        long start = System.currentTimeMillis();
        producerDAO.send(OperationTypeConstants.CREATE, RecordTypeConstants.WORKFLOW_DOC_TYPE, summary);
        Monitors.getTimer(Monitors.classQualifier, "kafka_produce_time", OperationTypeConstants.CREATE, RecordTypeConstants.WORKFLOW_DOC_TYPE).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    @Override
    public void indexTask(Task task) {
        TaskSummary summary = new TaskSummary(task);
        long start = System.currentTimeMillis();
        producerDAO.send(OperationTypeConstants.CREATE, RecordTypeConstants.TASK_DOC_TYPE, summary);
        Monitors.getTimer(Monitors.classQualifier, "kafka_produce_time", OperationTypeConstants.CREATE, RecordTypeConstants.TASK_DOC_TYPE).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs.isEmpty()) {
            return;
        }
        long start = System.currentTimeMillis();
        taskExecLogs.forEach(log -> producerDAO.send(OperationTypeConstants.CREATE, RecordTypeConstants.LOG_DOC_TYPE , taskExecLogs));
        Monitors.getTimer(Monitors.classQualifier, "kafka_produce_time", OperationTypeConstants.CREATE, RecordTypeConstants.LOG_DOC_TYPE).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
    }

    @Override
    public void removeWorkflow(String workflowId) {
        long start = System.currentTimeMillis();
        producerDAO.send(OperationTypeConstants.DELETE, RecordTypeConstants.WORKFLOW_DOC_TYPE, workflowId);
        Monitors.getTimer(Monitors.classQualifier, "kafka_produce_time", OperationTypeConstants.DELETE, RecordTypeConstants.WORKFLOW_DOC_TYPE).record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);

    }

}

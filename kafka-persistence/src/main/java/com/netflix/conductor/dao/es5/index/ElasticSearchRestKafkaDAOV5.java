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
import org.elasticsearch.client.RestClient;

import javax.inject.Inject;
import javax.inject.Singleton;


@Trace
@Singleton
public class ElasticSearchRestKafkaDAOV5 extends ElasticSearchRestDAOV5 {

    private ProducerDAO producerDAO;

    @Inject
    public ElasticSearchRestKafkaDAOV5(RestClient lowLevelRestClient, ElasticSearchConfiguration config, ObjectMapper objectMapper, ProducerDAO kafkaProducer) {
        super(lowLevelRestClient, config, objectMapper);
        this.producerDAO = kafkaProducer;

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

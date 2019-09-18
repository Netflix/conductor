package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.KafkaProducerDAO;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.elasticsearch.client.RestClient;

import javax.inject.Inject;
import javax.inject.Singleton;


@Trace
@Singleton
public class ElasticSearchRestKafkaDAOV5 extends ElasticSearchRestDAOV5 {

    private KafkaProducerDAO kafkaProducerDAO;

    @Inject
    public ElasticSearchRestKafkaDAOV5(RestClient lowLevelRestClient, ElasticSearchConfiguration config, ObjectMapper objectMapper, KafkaProducerDAO kafkaProducer) {
        super(lowLevelRestClient, config, objectMapper);
        this.kafkaProducerDAO = kafkaProducer;

    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        kafkaProducerDAO.produceWorkflow(workflow);
    }

    @Override
    public void indexTask(Task task) {
        kafkaProducerDAO.produceTask(task);
    }
}

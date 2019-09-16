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
import org.elasticsearch.client.RestClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;


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

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
       kafkaProducerDAO.produceTaskExecutionLogs(taskExecLogs);
    }

    @Override
    public void addEventExecution(EventExecution taskExecLogs) {
        kafkaProducerDAO.produceEventExecution(taskExecLogs);
    }

    @Override
    public void addMessage(String queue, Message message) {
        kafkaProducerDAO.produceMessage(queue, message);
    }
}

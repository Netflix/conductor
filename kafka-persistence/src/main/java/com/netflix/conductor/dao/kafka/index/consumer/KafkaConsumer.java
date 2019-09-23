package com.netflix.conductor.dao.kafka.index.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Singleton;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.ConsumerDAO;
import com.netflix.conductor.dao.kafka.index.constants.ConsumerConstants;
import com.netflix.conductor.dao.kafka.index.mapper.Mapper;
import com.netflix.conductor.dao.kafka.index.serialiser.DataDeSerializer;
import com.netflix.conductor.dao.kafka.index.serialiser.Record;
import com.netflix.conductor.dao.kafka.index.utils.DataUtils;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;


@Singleton
@Trace
public class KafkaConsumer implements ConsumerDAO {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
	private ObjectMapper om = Mapper.getObjectMapper();

	private String requestTimeoutConfig;
	private int pollInterval;
	private Consumer consumer;

	@Inject private String topic;
	ExecutorService scheduler;
	private IndexDAO indexDAO;
	private boolean shutdown;
	private ElasticSearchConfiguration elasticSearchConfiguration;

	@Inject
	public KafkaConsumer(ElasticSearchConfiguration elasticSearchConfiguration, IndexDAO indexDAO) {
		this.elasticSearchConfiguration = elasticSearchConfiguration;
		this.indexDAO = indexDAO;
	}

	public void init() {
		this.requestTimeoutConfig = elasticSearchConfiguration.getProperty(ConsumerConstants.KAFKA_REQUEST_TIMEOUT_MS, ConsumerConstants.DEFAULT_REQUEST_TIMEOUT);
		String requestTimeoutMs = requestTimeoutConfig;

		this.pollInterval = elasticSearchConfiguration.getIntProperty(ConsumerConstants.KAFKA_CONSUMER_POLL_INTERVAL, ConsumerConstants.CONSUMER_DEFAULT_POLL_INTERVAL);
		this.topic = elasticSearchConfiguration.getProperty(ConsumerConstants.KAFKA_CONSUMER_TOPIC, ConsumerConstants.CONSUMER_DEFAULT_TOPIC);

		Properties consumerConfig = new Properties();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, elasticSearchConfiguration.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConstants.DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConstants.STRING_DESERIALIZER);
		consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConsumerConstants.STRING_DESERIALIZER);
		consumerConfig.put("group.id",  "_group");
		consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(consumerConfig);

		consumer.subscribe(Collections.singleton(this.topic));

		scheduler = Executors.newSingleThreadExecutor();
		try {
			scheduler.submit(() -> consume());
		} catch(RejectedExecutionException e) {
			logger.error("Task Rejected in scheduler Exception {}", e);
			Monitors.getCounter(Monitors.classQualifier, "pending_tasks", "pending_tasks").increment();
		}
		SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(Record.class, new DataDeSerializer());
		om.registerModule(deserializeModule);
	}

	public void consume() {
		while(!shutdown) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(this.pollInterval));
				Monitors.getCounter(Monitors.classQualifier, "consumer_records", "consumer_records").increment(records.count());
				records.forEach(record -> consumeData(record.value()));
			} catch (KafkaException e) {
				logger.error("kafka KafkaConsumer message polling failed.", e);
				Monitors.getCounter(Monitors.classQualifier, "consumer_error", "consumer_error").increment();
			}
		}
	}

	@Override
	public void consumeData(String data) {
		try {
			Record d = om.readValue(data, Record.class);
			byte[] payload;
			long start = System.currentTimeMillis();
			switch (d.getType()) {
				case DataUtils.WORKFLOW_DOC_TYPE:
					payload = om.writeValueAsBytes(d.getPayload());
					consumeWorkflow(payload, om.readTree(data).get("workflowId").asText());
					break;
				case DataUtils.TASK_DOC_TYPE:
					payload = om.writeValueAsBytes(d.getPayload());
					consumeTask(payload, om.readTree(data).get("taskId").asText());
					break;
				default:
					break;
			}
			Monitors.getTimer(Monitors.classQualifier, "elastic_search_index_time", "elastic_search_index_time").record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
		} catch (IOException e) {
			// JSON is not formatted. Workflow details in UI won't be available.
			logger.error("Failed to consume from kafka - unknown exception:", e);
		}
	}

	private void consumeWorkflow(byte[] doc, String id) {
		try {
			WorkflowSummary workflowSummary  = om.readValue(new String(doc), WorkflowSummary.class);
			indexDAO.asyncIndexWorkflowSummary(workflowSummary);
		} catch (Exception e) {
			// JSON is not formatted. Workflow details in UI won't be available.
			logger.error("Failed to index workflow: {}", id, e);
		}
	}


	private void consumeTask(byte[] doc, String id) {
		try {
			TaskSummary taskSummary  = om.readValue(new String(doc), TaskSummary.class);
			indexDAO.asyncIndexTaskSummary(taskSummary);
		} catch (Exception e) {
			// JSON is not formatted. Workflow details in UI won't be available.
			logger.error("Failed to index task: {}", id, e);
		}
	}

	@Override
	public void close() {
		shutdown = true;
		scheduler.shutdownNow();
		try {
			scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		consumer.close();
	}

}


package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Singleton;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.KafkaConsumerDAO;
import com.netflix.conductor.dao.kafka.index.serialiser.DataDeSerializer;
import com.netflix.conductor.dao.kafka.index.serialiser.Record;
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
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;


@Singleton
@Trace
public class KafkaConsumer implements KafkaConsumerDAO {

	public static final String KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
	public static final String KAFKA_CONSUMER_POLL_INTERVAL = "kafka.consumer.poll.interval.ms";
	public static final String KAFKA_CONSUMER_TOPIC = "kafka.consumer.topic";
	public static final String CONSUMER_DEFAULT_TOPIC = "mytest";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";
	public static final int CONSUMER_DEFAULT_POLL_INTERVAL = 1;
	public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
	private static final String KAFKA_CONSUMER_THREADS = "kafka.consumer.threads";
	private static final int DEFAULT_KAFKA_CONSUMER_THREADS = 1;
	private ObjectMapper om = Record.objectMapper();

	public String requestTimeoutConfig;
	public int pollInterval;
	private Consumer consumer;

	@Inject private String topic;
	private int threads;
	ScheduledExecutorService scheduler;
	private IndexDAO indexDAO;

	@Inject
	public void KafkaConsumer(Configuration configuration, IndexDAO indexDAO) {
		this.indexDAO = indexDAO;
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		String requestTimeoutMs = requestTimeoutConfig;

		this.pollInterval = configuration.getIntProperty(KAFKA_CONSUMER_POLL_INTERVAL, CONSUMER_DEFAULT_POLL_INTERVAL);
		this.topic = configuration.getProperty(KAFKA_CONSUMER_TOPIC, CONSUMER_DEFAULT_TOPIC);
		this.threads = configuration.getIntProperty(KAFKA_CONSUMER_THREADS, DEFAULT_KAFKA_CONSUMER_THREADS);

		Properties consumerConfig = new Properties();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
		consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
		consumerConfig.put("group.id",  "_group");
		consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(consumerConfig);
		consumer.subscribe(Collections.singleton(this.topic));

		scheduler = Executors.newScheduledThreadPool(this.threads);
		try {
			scheduler.scheduleAtFixedRate(() -> consume(), 0, this.pollInterval, TimeUnit.MILLISECONDS);
		} catch(RejectedExecutionException e) {
			Monitors.getCounter(Monitors.classQualifier, "pending_tasks", "pending_tasks").increment();
		}
		SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(Record.class, new DataDeSerializer());
		om.registerModule(deserializeModule);
	}

	public void consume() {
			try {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				Monitors.getCounter(Monitors.classQualifier, "consumer_records", "consumer_records").increment(records.count());
				logger.info("polled {} messages from kafka topic.", records.count());
				records.forEach(record -> {
					try {
						Record d = om.readValue(record.value(), Record.class);
						byte[] data;
						long start = System.currentTimeMillis();
						switch (d.getType()) {
							case KafkaProducer.WORKFLOW_DOC_TYPE:
								data = om.writeValueAsBytes(d.getPayload());
								consumeWorkflow(data, d.getType(), om.readTree(data).get("workflowId").asText());
								break;
							case KafkaProducer.TASK_DOC_TYPE:
								data = om.writeValueAsBytes(d.getPayload());
								consumeTask(data, d.getType(), om.readTree(data).get("taskId").asText());
								break;
							case KafkaProducer.LOG_DOC_TYPE:
								consumeTaskExecutionLog(d.getType(), d.getPayload());
								break;
							case KafkaProducer.EVENT_DOC_TYPE:
								consumeEventExecution(d.getPayload(), d.getType());
								break;
							case KafkaProducer.MSG_DOC_TYPE:
								consumeMessage(om.convertValue(d.getPayload(), Map.class));
								break;
							default:
								break;
						}
						Monitors.getTimer(Monitors.classQualifier, "elastic_search_index_time", "elastic_search_index_time").record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
					} catch (IOException e) {
						logger.debug(e.getMessage());
					}
				});
			} catch (KafkaException e) {
				logger.error("kafka consumer message polling failed.", e);
				Monitors.getCounter(Monitors.classQualifier, "consumer_error", "consumer_error").increment();
			}
	}


	@Override
	public void consumeWorkflow(byte[] doc, String docType, String id) {
		try {
			WorkflowSummary workflowSummary  = om.readValue(new String(doc), WorkflowSummary.class);
			indexDAO.asyncIndexWorkflowSummary(workflowSummary);
		} catch (Exception e) {
			logger.error("Failed to index workflow: {}", id, e);
		}
	}


	@Override
	public void consumeTask(byte[] doc, String docType, String id) {
		try {
			TaskSummary taskSummary  = om.readValue(new String(doc), TaskSummary.class);
			indexDAO.asyncIndexTaskSummary(taskSummary);
		} catch (Exception e) {
			logger.error("Failed to index task: {}", id, e);
		}
	}

	@Override
	public void consumeTaskExecutionLog(String type, Object taskExecLog) {

		try {
			TaskExecLog log  = om.readValue(new String(om.writeValueAsBytes(taskExecLog)), TaskExecLog.class);
			indexDAO.asyncAddTaskExecutionLogs(Arrays.asList(log));
		} catch (Exception e) {
            logger.error("Failed to index task execution logs for tasks: {}", taskExecLog, e);
		}
	}

	@Override
	public void consumeMessage(Map message) {
		try {
			indexDAO.addMessage(String.valueOf(message.get("queue")), om.readValue(message.toString(), Message.class));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void consumeEventExecution(Object data, String eventExecution) {
		try {
			EventExecution event  = om.readValue(new String(om.writeValueAsBytes(data)), EventExecution.class);
			indexDAO.asyncAddEventExecution(event);
		} catch (Exception e) {
			logger.error("Failed to index event execution: {}", eventExecution, e);
		}
	}

}


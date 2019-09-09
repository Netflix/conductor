package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.KafkaProducerDAO;
import com.netflix.conductor.dao.kafka.index.serialiser.DataSerializer;
import com.netflix.conductor.dao.kafka.index.serialiser.Record;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Trace
@Singleton
public class KafkaProducer implements KafkaProducerDAO {

	public static final String KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String KAFKA_PRODUCER_TOPIC = "kafka.producer.topic";
	public static final String PRODUCER_DEFAULT_TOPIC = "test";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";
	public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	public static final String WORKFLOW_DOC_TYPE = "workflow";
	public static final String TASK_DOC_TYPE = "task";
	public static final String LOG_DOC_TYPE = "task_log";
	public static final String EVENT_DOC_TYPE = "event";
	public static final String MSG_DOC_TYPE = "message";
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private final String topic;
	private ObjectMapper om = Record.objectMapper();

	public final String requestTimeoutConfig;
	private Producer producer;

	@Inject
	public KafkaProducer(Configuration configuration) {
		this.topic = configuration.getProperty(KAFKA_PRODUCER_TOPIC, PRODUCER_DEFAULT_TOPIC);
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
		String requestTimeoutMs = requestTimeoutConfig;
		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
		producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerConfig);

		SimpleModule serializeModule = new SimpleModule();
		serializeModule.addSerializer(Record.class, new DataSerializer());
		om.registerModule(serializeModule);

	}

	public void send(String t, Object value) {
		try {
			long start = System.currentTimeMillis();
			Record d = new Record(t, value);
			ProducerRecord rec = new ProducerRecord(this.topic, om.writeValueAsString(d));
			Future<RecordMetadata> recordMetaDataFuture = producer.send(rec);
			recordMetaDataFuture.get();
			Monitors.getTimer(Monitors.classQualifier, "kafka_produce_time", "").record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			logger.error("Failed to publish to kafka - unknown exception:", e);
			Monitors.getCounter(Monitors.classQualifier, "kafka_publishing_error", "").increment();
		}
	}

	@Override
	public void produceWorkflow(Workflow workflow) {
		try {
			WorkflowSummary summary = new WorkflowSummary(workflow);
			send(WORKFLOW_DOC_TYPE, summary);
		} catch (Exception e) {
			logger.error("Failed to index workflow: {}", workflow.getWorkflowId(), e);
		}
	}

	@Override
	public void produceTask(Task task) {
		try {
			TaskSummary summary = new TaskSummary(task);
			send(TASK_DOC_TYPE, summary);
		} catch (Exception e) {
			logger.error("Failed to index task: {}", task.getTaskId(), e);
		}
	}

	@Override
	public void produceTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
		if (taskExecLogs.isEmpty()) {
			return;
		}

		for (TaskExecLog log : taskExecLogs) {
			send(LOG_DOC_TYPE, log);
		}
	}

	@Override
	public void produceMessage(String queue, Message message) {
		Map<String, Object> doc = new HashMap<>();
		doc.put("messageId", message.getId());
		doc.put("payload", message.getPayload());
		doc.put("queue", queue);
		doc.put("created", System.currentTimeMillis());
		send(MSG_DOC_TYPE, doc);
	}

	@Override
	public void produceEventExecution(EventExecution eventExecution) {
		send(EVENT_DOC_TYPE, eventExecution);
	}

}


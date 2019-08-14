package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.KafkaConsumeDAO;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@Trace
@Singleton
public class KafkaConsumer implements KafkaConsumeDAO {

	public static final String KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
	public static final String KAFKA_CONSUMER_POLL_INTERVAL = "kafka.consumer.poll.interval.ms";
	public static final String KAFKA_CONSUMER_TOPIC = "kafka.consumer.topic";
	public static final String CONSUMER_DEFAULT_TOPIC = "test";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";
	public static final int CONSUMER_DEFAULT_POLL_INTERVAL = 1;
	public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
	private static final String KAFKA_CONSUMER_THREADS = "kafka.consumer.threads";
	private static final int DEFAULT_KAFKA_CONSUMER_THREADS = 1;
	private ObjectMapper om = Record.objectMapper();

	public final String requestTimeoutConfig;
	public final int pollInterval;
	private Consumer consumer;
	private final IndexDAO indexDAO;
	private final String topic;
	private final int threads;
	final ScheduledExecutorService scheduler;

	@Inject
	public KafkaConsumer(Configuration configuration, IndexDAO indexDAO) {
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

	@Override
	public void consume() {
			try {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				Monitors.getGauge(Monitors.classQualifier, "consumer_records", "consumer_records").set(records.count());
				logger.info("polled {} messages from kafka topic.", records.count());
				records.forEach(record -> {
					logger.debug(
							"Consumer Record: " + "key: {}, " + "value: {}, " + "partition: {}, " + "offset: {}",
							record.key(), record.value(), record.partition(), record.offset());
					try {
						Record d = om.readValue(record.value(), Record.class);
						byte[] data;
						long start = System.currentTimeMillis();
						switch (d.type) {
							case "workflow":
								data = om.writeValueAsBytes(d.payload);
								indexDAO.consumeWorkflow(data, d.type, om.readTree(data).get("workflowId").asText());
								break;
							case "task":
								data = om.writeValueAsBytes(d.payload);
								indexDAO.consumeTask(data, d.type, om.readTree(data).get("taskId").asText());
								break;
							case "task_log":
								indexDAO.consumeTaskExecutionLog(d.type, d.payload);
								break;
							case "event":
								indexDAO.consumeEventExecution(d.payload, d.type);
								break;
							case "message":
								indexDAO.consumeMessage(d.type, om.convertValue(d.payload, Map.class));
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
}

class DataDeSerializer extends StdDeserializer<Record> {

	public DataDeSerializer() {
		this(null);
	}

	@Override
	public Record deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
		JsonNode node = jp.getCodec().readTree(jp);
		String type = node.get("type").asText();
		Object payload = node.get("payload");

		return new Record(type, payload);
	}

	public DataDeSerializer(Class<Record> t) {
		super(t);
	}
}


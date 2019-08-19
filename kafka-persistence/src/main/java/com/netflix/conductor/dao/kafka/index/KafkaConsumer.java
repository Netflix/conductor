package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Singleton;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.utils.RetryUtil;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.KafkaConsumerDAO;
import com.netflix.conductor.dao.kafka.index.serialiser.DataDeSerializer;
import com.netflix.conductor.dao.kafka.index.serialiser.Record;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.SystemPropertiesElasticSearchConfiguration;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import javax.inject.Inject;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static com.netflix.conductor.dao.kafka.index.KafkaProducer.*;

@Singleton
@Trace
public class KafkaConsumer implements KafkaConsumerDAO {

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
	private static final String className = KafkaConsumerDAO.class.getSimpleName();
	private ObjectMapper om = Record.objectMapper();
	private static final int RETRY_COUNT = 3;
	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyyMMww");

	public String requestTimeoutConfig;
	public int pollInterval;
	private Consumer consumer;

	@Inject private String topic;
	private int threads;
	ScheduledExecutorService scheduler;
	private String logIndexName;
	private String indexName;
	private Client elasticSearchClient;

	@Inject
	public void KafkaConsumer(Configuration configuration, Client elasticSearchClient, ElasticSearchConfiguration config) {
		this.elasticSearchClient = elasticSearchClient;
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
		this.indexName = config.getIndexName();
		this.logIndexName = config.getTasklogIndexName() + "_" + SIMPLE_DATE_FORMAT.format(new Date());
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
				Monitors.getGauge(Monitors.classQualifier, "consumer_records", "consumer_records").set(records.count());
				logger.info("polled {} messages from kafka topic.", records.count());
				records.forEach(record -> {
					try {
						Record d = om.readValue(record.value(), Record.class);
						byte[] data;
						long start = System.currentTimeMillis();
						switch (d.getType()) {
							case WORKFLOW_DOC_TYPE:
								data = om.writeValueAsBytes(d.getPayload());
								consumeWorkflow(data, d.getType(), om.readTree(data).get("workflowId").asText());
								break;
							case TASK_DOC_TYPE:
								data = om.writeValueAsBytes(d.getPayload());
								consumeTask(data, d.getType(), om.readTree(data).get("taskId").asText());
								break;
							case LOG_DOC_TYPE:
								consumeTaskExecutionLog(d.getType(), d.getPayload());
								break;
							case EVENT_DOC_TYPE:
								consumeEventExecution(d.getPayload(), d.getType());
								break;
							case MSG_DOC_TYPE:
								consumeMessage(d.getType(), om.convertValue(d.getPayload(), Map.class));
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
			UpdateRequest req = new UpdateRequest(indexName, docType, id);
			req.doc(doc, XContentType.JSON);
			req.upsert(doc, XContentType.JSON);
			req.retryOnConflict(5);
			updateWithRetry(req, "Index workflow into doc_type workflow");
		} catch (Exception e) {
			logger.error("Failed to index workflow: {}", id, e);
		}
	}


	@Override
	public void consumeTask(byte[] doc, String docType, String id) {
		try {
			UpdateRequest req = new UpdateRequest(indexName, docType, id);
			req.doc(doc, XContentType.JSON);
			req.upsert(doc, XContentType.JSON);
			updateWithRetry(req, "Index workflow into doc_type workflow");
		} catch (Exception e) {
			logger.error("Failed to index task: {}", id, e);
		}
	}

	@Override
	public void consumeTaskExecutionLog(String type, Object taskExecLog) {

		try {
			IndexRequest request = new IndexRequest(logIndexName, type);
			request.source(om.writeValueAsBytes(taskExecLog), XContentType.JSON);
			new RetryUtil<IndexResponse>().retryOnException(
					() -> elasticSearchClient.index(request).actionGet(),
					null,
					null,
					RETRY_COUNT,
					"Indexing all execution logs into doc_type task",
					"addTaskExecutionLogs"
			);
		} catch (Exception e) {
            logger.error("Failed to index task execution logs for tasks: {}", taskExecLog, e);
		}
	}

	@Override
	public void consumeMessage(String type, Map message) {
		IndexRequest request = new IndexRequest(logIndexName, type);
		request.source(message);

	}

	@Override
	public void consumeEventExecution(Object data, String eventExecution) {
		try {
			byte[] doc = om.writeValueAsBytes(data);
			UpdateRequest req = new UpdateRequest(logIndexName, EVENT_DOC_TYPE, eventExecution);
			req.doc(doc, XContentType.JSON);
			req.upsert(doc, XContentType.JSON);
			req.retryOnConflict(5);
			updateWithRetry(req, "Update Event execution for doc_type event");
		} catch (Exception e) {
			logger.error("Failed to index event execution: {}", eventExecution, e);
		}
	}

	public void updateWithRetry(UpdateRequest request, String operationDescription) {
		try {
			new RetryUtil<UpdateResponse>().retryOnException(
					() -> elasticSearchClient.update(request).actionGet(),
					null,
					null,
					RETRY_COUNT,
					operationDescription,
					"updateWithRetry"
			);
		} catch (Exception e) {
			Monitors.error(className, "index");
			logger.error("Failed to index {} for request type: {}", request.index(), request.type(),
					e);
		}
	}

}


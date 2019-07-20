package com.netflix.conductor.contribs.kafka;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Singleton
public class KafkaPublishTask extends WorkflowSystemTask {

	static final String REQUEST_PARAMETER_NAME = "kafka_request";
	private static final String NAME = "KAFKA_PUBLISH";
	private static final String MISSING_REQUEST = "Missing Kafka request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key with KafkaTask.Input as value. See documentation for KafkaTask for required input parameters";
	private static final String MISSING_BOOT_STRAP_SERVERS = "No boot strap servers specified";
	private static final String MISSING_KAFKA_TOPIC = "Missing Kafka topic. See documentation for KafkaTask for required input parameters";
	private static final String MISSING_KAFKA_VALUE = "Missing Kafka value.  See documentation for KafkaTask for required input parameters";
	private static final String FAILED_TO_INVOKE = "Failed to invoke kafka task due to: ";

	private ObjectMapper om = objectMapper();
	private Configuration config;
	private String requestParameter;
	KafkaProducerManager producerManager;

	private static final Logger logger = LoggerFactory.getLogger(KafkaPublishTask.class);


	@Inject
	public KafkaPublishTask(Configuration config, KafkaProducerManager clientManager) {
		super(NAME);
		this.config = config;
		this.requestParameter = REQUEST_PARAMETER_NAME;
		this.producerManager = clientManager;
		logger.info("KafkaTask initialized...");
	}

	private static ObjectMapper objectMapper() {
		final ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		return om;
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) {

		long taskStartMillis = Instant.now().toEpochMilli();
		task.setWorkerId(config.getServerId());
		Object request = task.getInputData().get(requestParameter);

		if (Objects.isNull(request)) {
			markTaskAsFailed(task, MISSING_REQUEST);
			return;
		}

		KafkaPublishTask.Input input = om.convertValue(request, KafkaPublishTask.Input.class);

		if (StringUtils.isBlank(input.getBootStrapServers())) {
			markTaskAsFailed(task, MISSING_BOOT_STRAP_SERVERS);
			return;
		}

		if (StringUtils.isBlank(input.getTopic())) {
			markTaskAsFailed(task, MISSING_KAFKA_TOPIC);
			return;
		}

		if (Objects.isNull(input.getValue())) {
			markTaskAsFailed(task, MISSING_KAFKA_VALUE);
			return;
		}

		try {
			Future<RecordMetadata> recordMetaDataFuture = kafkaPublish(input);
			try {
				recordMetaDataFuture.get();
				task.setStatus(Task.Status.COMPLETED);
				long timeTakenToCompleteTask = Instant.now().toEpochMilli() - taskStartMillis;
				logger.info("Published message {}, Time taken {}", input, timeTakenToCompleteTask);

			} catch (ExecutionException ec) {
				logger.error("Failed to invoke kafka task - execution exception {}", ec);
				markTaskAsFailed(task, FAILED_TO_INVOKE + ec.getMessage());
			}
		} catch (Exception e) {
			logger.error(String.format("Failed to invoke kafka task for input {} - unknown exception: {}", input), e);
			markTaskAsFailed(task, FAILED_TO_INVOKE + e.getMessage());
		}
	}

	private void markTaskAsFailed(Task task, String reasonForIncompletion) {
		task.setReasonForIncompletion(reasonForIncompletion);
		task.setStatus(Task.Status.FAILED);
	}

	/**
	 * @param input Kafka Request
	 * @return Future for execution.
	 */
	 private Future<RecordMetadata> kafkaPublish(KafkaPublishTask.Input input) throws Exception {

		long startPublishingEpochMillis = Instant.now().toEpochMilli();

		Producer producer = producerManager.getProducer(input);

		long timeTakenToCreateProducer = Instant.now().toEpochMilli() - startPublishingEpochMillis;

		logger.debug("Time taken getting producer {}", timeTakenToCreateProducer);

		Object key = getKey(input);

		Iterable<Header> headers = input.getHeaders().entrySet().stream()
				.map(header -> new RecordHeader(header.getKey(), String.valueOf(header.getValue()).getBytes()))
				.collect(Collectors.toList());
		ProducerRecord rec = new ProducerRecord(input.getTopic(), null,
				null, key, om.writeValueAsString(input.getValue()), headers);

		Future send = producer.send(rec);
		producer.close();

		long timeTakenToPublish = Instant.now().toEpochMilli() - startPublishingEpochMillis;

		logger.debug("Time taken publishing {}", timeTakenToPublish);

		return send;
	}

	@VisibleForTesting
	Object getKey(Input input) {

		String keySerializer = input.getKeySerializer();

		if (LongSerializer.class.getCanonicalName().equals(keySerializer)) {
			return Long.parseLong(String.valueOf(input.getKey()));
		} else if (IntegerSerializer.class.getCanonicalName().equals(keySerializer)) {
			return Integer.parseInt(String.valueOf(input.getKey()));
		} else {
			return String.valueOf(input.getKey());
		}

	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
		return false;
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
		task.setStatus(Task.Status.CANCELED);
	}

	@Override
	public boolean isAsync() {
		return true;
	}


	public static class Input {

		public static final String STRING_SERIALIZER = StringSerializer.class.getCanonicalName();
		private Map<String, Object> headers = new HashMap<>();

		private String bootStrapServers;

		private Object key;

		private Object value;

		private Integer requestTimeoutMs;

		private String topic;

		private String keySerializer = STRING_SERIALIZER;

		public Map<String, Object> getHeaders() {
			return headers;
		}

		public void setHeaders(Map<String, Object> headers) {
			this.headers = headers;
		}

		public String getBootStrapServers() {
			return bootStrapServers;
		}

		public void setBootStrapServers(String bootStrapServers) {
			this.bootStrapServers = bootStrapServers;
		}

		public Object getKey() {
			return key;
		}

		public void setKey(Object key) {
			this.key = key;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public Integer getRequestTimeoutMs() {
			return requestTimeoutMs;
		}

		public void setRequestTimeoutMs(Integer requestTimeoutMs) {
			this.requestTimeoutMs = requestTimeoutMs;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getKeySerializer() {
			return keySerializer;
		}

		public void setKeySerializer(String keySerializer) {
			this.keySerializer = keySerializer;
		}

		@Override
		public String toString() {
			return "Input{" +
					"headers=" + headers +
					", bootStrapServers='" + bootStrapServers + '\'' +
					", value=" + value +
					", requestTimeoutMs=" + requestTimeoutMs +
					", topic='" + topic + '\'' +
					", keySerializer='" + keySerializer + '\'' +
					'}';
		}
	}
}

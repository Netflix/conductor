package com.netflix.conductor.contribs.confluent_kafka;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.swiggy.kafka.clients.Record;
import com.swiggy.kafka.clients.producer.CallbackFuture;
import com.swiggy.kafka.clients.producer.Producer;
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

@Singleton
public class ConfluentKafkaPublishTask extends WorkflowSystemTask {

	public static final String REQUEST_PARAMETER_NAME = "kafka_request";
	@VisibleForTesting
	static final String NAME = "CONFLUENT_KAFKA_PUBLISH";
	@VisibleForTesting
	static final String MISSING_REQUEST = "Missing Kafka request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key with KafkaTask.Input as value. See documentation for KafkaTask for required input parameters";
	@VisibleForTesting
	static final String MISSING_KAFKA_TOPIC = "Missing Kafka topic. See documentation for KafkaTask for required input parameters";
	@VisibleForTesting
	static final String MISSING_KAFKA_VALUE = "Missing Kafka value.  See documentation for KafkaTask for required input parameters";
	@VisibleForTesting
	static final String FAILED_TO_INVOKE = "Failed to invoke kafka task due to: ";
	@VisibleForTesting
	static final String MISSING_KAFKA_TOPIC_NAME = "Missing kafka topic name";
	@VisibleForTesting
	static final String MISSING_KAFKA_TOPIC_FAULT_STRETAGY = "Missing kafka topic fault strategy";
	@VisibleForTesting
	static final String MISSING_KAFKA_TOPIC_ENABLE_ENCRYPTION = "Missing kafka topic enable encryption";
	@VisibleForTesting
	static final String MISSING_KAFKA_TOPIC_KEY_ID = "Missing kafka topic key id";
	@VisibleForTesting
	static final String MISSING_PRIMARY_CLUSTER = "Missing primary cluster information";
	@VisibleForTesting
	static final String MISSING_PRIMARY_CLUSTER_BOOT_STRAP_SERVERS = "Missing primary cluster boot strap server information";
	@VisibleForTesting
	static final String MISSING_PRIMARY_CLUSTER_AUTH_MECHANISM = "Missing primary cluster auth mechanism";
	@VisibleForTesting
	static final String MISSING_SECONDARY_CLUSTER_BOOT_STRAP_SERVERS = "Missing secondary cluster boot auth mechanism";
	@VisibleForTesting
	static final String MISSING_SECONDARY_CLUSTER_AUTH_MECHANISM = "Missing primary cluster auth mechanism";
	@VisibleForTesting
	static final String MISSING_CLIENT_ID = "Missing producer client id";
	@VisibleForTesting
	static final String MISSING_PRODUCER_ACK = "Missing producer ACK information";


	private ObjectMapper objectMapper;
	private Configuration config;
	private String requestParameter;
	ConfluentKafkaProducerManager confluentKafkaProducerManager;

	private static final Logger logger = LoggerFactory.getLogger(ConfluentKafkaPublishTask.class);


	@Inject
	public ConfluentKafkaPublishTask(Configuration config, ConfluentKafkaProducerManager confluentKafkaProducerManager, ObjectMapper objectMapper) {
		super(NAME);
		this.config = config;
		this.requestParameter = REQUEST_PARAMETER_NAME;
		this.confluentKafkaProducerManager = confluentKafkaProducerManager;
		this.objectMapper = objectMapper;
		logger.info("ConfluentKafkaPublishTask initialized...");
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

		ConfluentKafkaPublishTask.Input input = objectMapper.convertValue(request, ConfluentKafkaPublishTask.Input.class);

		if (input.getTopic() == null) {
			markTaskAsFailed(task, MISSING_KAFKA_TOPIC);
			return;
		} else {
			Map<String, Object> topic = input.getTopic();
			if (topic.get("name") == null) {
				markTaskAsFailed(task, MISSING_KAFKA_TOPIC_NAME);
				return;
			} else if (topic.get("faultStrategy") == null) {
				markTaskAsFailed(task, MISSING_KAFKA_TOPIC_FAULT_STRETAGY);
				return;
			} else if (topic.get("enableEncryption") == null) {
				markTaskAsFailed(task, MISSING_KAFKA_TOPIC_ENABLE_ENCRYPTION);
				return;
			} else if (topic.get("keyId") == null) {
				markTaskAsFailed(task, MISSING_KAFKA_TOPIC_KEY_ID);
				return;
			}
		}

		if (input.getPrimaryCluster() == null) {
			markTaskAsFailed(task, MISSING_PRIMARY_CLUSTER);
			return;
		} else {
			Map<String, Object> primaryCluster = input.getPrimaryCluster();
			if (primaryCluster.get("bootStrapServers") == null) {
				markTaskAsFailed(task, MISSING_PRIMARY_CLUSTER_BOOT_STRAP_SERVERS);
				return;
			} else if (primaryCluster.get("authMechanism") == null) {
				markTaskAsFailed(task, MISSING_PRIMARY_CLUSTER_AUTH_MECHANISM);
				return;
			}
		}

		if (input.getSecondaryCluster() != null) {
			Map<String, Object> secondaryCluster = input.getSecondaryCluster();
			if (secondaryCluster.get("bootStrapServers") == null) {
				markTaskAsFailed(task, MISSING_SECONDARY_CLUSTER_BOOT_STRAP_SERVERS);
				return;
			} else if (secondaryCluster.get("authMechanism") == null) {
				markTaskAsFailed(task, MISSING_SECONDARY_CLUSTER_AUTH_MECHANISM);
				return;
			}
		}

		if (input.getClientId() == null) {
			markTaskAsFailed(task, MISSING_CLIENT_ID);
			return;
		}

		if (input.getAcks() == null) {
			markTaskAsFailed(task, MISSING_PRODUCER_ACK);
			return;
		}

		if (Objects.isNull(input.getValue())) {
			markTaskAsFailed(task, MISSING_KAFKA_VALUE);
			return;
		}

		try {
			CallbackFuture<Record> record = kafkaPublish(input, task.getTaskDefName());
			try {
				record.get();
				task.setStatus(Task.Status.COMPLETED);
				long timeTakenToCompleteTask = Instant.now().toEpochMilli() - taskStartMillis;
				logger.debug("Published message {}, Time taken {}", input, timeTakenToCompleteTask);

			} catch (ExecutionException ec) {
				logger.error("Failed to invoke kafka task: {} - execution exception ", task.getTaskId(), ec);
				markTaskAsFailed(task, FAILED_TO_INVOKE + ec.getMessage());
			}
		} catch (Exception e) {
			logger.error("Failed to invoke kafka task:{} for input {} - unknown exception", task.getTaskId(), input, e);
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
	@SuppressWarnings("unchecked")
	private CallbackFuture<Record> kafkaPublish(ConfluentKafkaPublishTask.Input input, String taskDefName) throws Exception {

		long startPublishingEpochMillis = Instant.now().toEpochMilli();
		Producer producer = confluentKafkaProducerManager.getProducer(input, taskDefName);
		long timeTakenToCreateProducer = Instant.now().toEpochMilli() - startPublishingEpochMillis;
		logger.debug("Time taken getting producer {}", timeTakenToCreateProducer);
		return producer.send(String.valueOf(input.getTopic().get("name")), input.getKey(), objectMapper.writeValueAsString(input.getValue()), input.getHeaders());
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
		private Map<String, String> headers = new HashMap<>();

		private Map<String, Object> topic;

		@JsonProperty("primary_producer")
		private Map<String, Object> primaryCluster;

		@JsonProperty("secondary_producer")
		private Map<String, Object> secondaryCluster;

		@JsonProperty("producer_clientId")
		private String clientId;

		@JsonProperty("confluent_cluster_type")
		private String clusterType;

		@JsonProperty("retries")
		private int retries;

		@JsonProperty("producer_enableCompression")
		private boolean enableCompression;

		@JsonProperty("producer_acks")
		private String acks;

		private String key;

		private Object value;

		private String keySerializer = STRING_SERIALIZER;

		public Map<String, Object> getPrimaryCluster() {
			return primaryCluster;
		}

		public void setPrimaryCluster(Map<String, Object> primaryCluster) {
			this.primaryCluster = primaryCluster;
		}

		public void setSecondaryCluster(Map<String, Object> secondaryCluster) {
			this.secondaryCluster = secondaryCluster;
		}

		public Map<String, Object> getSecondaryCluster() {
			return secondaryCluster;
		}

		public String getClientId() {
			return clientId;
		}

		public void setClientId(String clientId) {
			this.clientId = clientId;
		}

		public boolean isEnableCompression() {
			return enableCompression;
		}

		public int getRetries() {
			return retries;
		}

		public String getAcks() {
			return acks;
		}

		public void setAcks(String ack) {
			this.acks = ack;
		}

		public Map<String, String> getHeaders() {
			return headers;
		}
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			 this.key = key;
		}
		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			 this.value = value;
		}

		public Map<String, Object> getTopic() {
			return topic;
		}

		public void setTopic(Map<String, Object> topic) {
			this.topic = topic;
		}

		public String getClusterType() {
			return clusterType;
		}

		public void setClusterType(String clusterType) {
			this.clusterType = clusterType;
		}

		@Override
		public String toString() {
			return "Input{" +
					"headers=" + headers +
					", key=" + key +
					", value=" + value +
					", topic='" + topic + '\'' +
					", keySerializer='" + keySerializer + '\'' +
					'}';
		}
	}
}

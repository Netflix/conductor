package com.netflix.conductor.contribs.confluent_kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.netflix.conductor.core.config.Configuration;
import com.swiggy.kafka.clients.configs.AuthMechanism;
import com.swiggy.kafka.clients.configs.CommonConfig;
import com.swiggy.kafka.clients.configs.ProducerConfig;
import com.swiggy.kafka.clients.configs.Topic;
import com.swiggy.kafka.clients.configs.enums.ProducerAcks;
import com.swiggy.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ConfluentKafkaProducerManager {

	public static final String KAFKA_PUBLISH_REQUEST_TIMEOUT_MS = "kafka.publish.request.timeout.ms";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";
	private static final String KAFKA_PRODUCER_CACHE_TIME_IN_MILLIS = "kafka.publish.producer.cache.time.ms" ;
	private static final int DEFAULT_CACHE_SIZE = 10;
	private static final String KAFKA_PRODUCER_CACHE_SIZE = "kafka.publish.producer.cache.size";
	private static final int DEFAULT_CACHE_TIME_IN_MILLIS = 120000;
	private Configuration configuration;
	private static final String KAFKA_PREFIX = "KAFKA";
	public enum ClusterType {
		BATCH("BATCH_PRIMARY_API_KEY", "BATCH_PRIMARY_API_SECRET"),
		TXN("TXN_PRIMARY_API_KEY", "TXN_PRIMARY_API_SECRET"),
		TXN_HA_PRIMARY("TXN_HA_PRIMARY_API_KEY", "TXN_HA_PRIMARY_API_SECRET"),
		TXN_HA_SECONDARY("TXN_HA_SECONDARY_API_KEY", "TXN_HA_SECONDARY_API_SECRET");

		private String  key;

		private String secret;

		ClusterType(String key, String secret){
			this.key = key;
			this.secret = secret;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ConfluentKafkaProducerManager.class);

	public static final RemovalListener<Properties, Producer> LISTENER = notification -> {
		notification.getValue().stop();
		logger.info("Closed producer for {}",notification.getKey());
	};


	@VisibleForTesting
	final String requestTimeoutConfig;
	private Cache<Properties, Producer> kafkaProducerCache;

	public ConfluentKafkaProducerManager(Configuration configuration) {
		this.configuration = configuration;
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_PUBLISH_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		int cacheSize = configuration.getIntProperty(KAFKA_PRODUCER_CACHE_SIZE, DEFAULT_CACHE_SIZE);
		int cacheTimeInMs = configuration.getIntProperty(KAFKA_PRODUCER_CACHE_TIME_IN_MILLIS, DEFAULT_CACHE_TIME_IN_MILLIS);
		this.kafkaProducerCache = CacheBuilder.newBuilder().removalListener(LISTENER)
				.maximumSize(cacheSize).expireAfterAccess(cacheTimeInMs, TimeUnit.MILLISECONDS)
				.build();
	}


	public Producer getProducer(ConfluentKafkaPublishTask.Input input, String taskDefName, String workflowName) {

		ProducerConfig producerConfig = getProducerProperties(input, taskDefName, workflowName);


		return getFromCache(getCacheProperties(taskDefName, workflowName,  input), () -> {
				Producer producer = new Producer(producerConfig);
				producer.start();
				return producer;
		});

	}

	@VisibleForTesting
	Producer getFromCache(Properties properties, Callable<Producer> createProducerCallable) {
		try {
			return kafkaProducerCache.get(properties, createProducerCallable);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	Properties getCacheProperties(String taskDefName, String workflowname, ConfluentKafkaPublishTask.Input input) {
		Properties properties = new Properties();
		properties.put("topic_name", String.valueOf(input.getTopic().get("name")));
		properties.put("task_name", taskDefName);
		properties.put("workflow_name", workflowname);
		properties.put("primary_bootstrap_server", String.valueOf(input.getPrimaryCluster().get("bootStrapServers")));
		if (!(input.getClusterType().equals(ClusterType.BATCH.name()) || input.getClusterType().equals(ClusterType.TXN.name()))) {
			properties.put("secondary_bootstrap_server", String.valueOf(input.getPrimaryCluster().get("bootStrapServers")));
		}
		properties.put("client_id", input.getClientId());
		return properties;
	}

	@VisibleForTesting
	ProducerConfig getProducerProperties(ConfluentKafkaPublishTask.Input input, String taskDefName, String workflowName) {

		Map<String, Topic> topics = new HashMap<>();
        Topic topic = getTopic(input);
		topics.put(topic.getName(), topic);

		CommonConfig.Cluster primaryCluster =  new CommonConfig.Cluster();
		CommonConfig.Cluster secondaryCluster = null;
		primaryCluster.setBootstrapServers(String.valueOf(input.getPrimaryCluster().get("bootStrapServers")));
		primaryCluster.setAuthMechanism(AuthMechanism.valueOf(String.valueOf(input.getPrimaryCluster().get("authMechanism"))));
		if (input.getClusterType().equals(ClusterType.BATCH.name()) || input.getClusterType().equals(ClusterType.TXN.name())) {
			primaryCluster.setUsername(getTaskKey(taskDefName, input.getClusterType(), workflowName));
			primaryCluster.setPassword(getTaskSecret(taskDefName,input.getClusterType(), workflowName));
		} else {
			primaryCluster.setUsername(getTaskKey(taskDefName, ClusterType.TXN_HA_PRIMARY.name(), workflowName));
			primaryCluster.setPassword(getTaskSecret(taskDefName, ClusterType.TXN_HA_PRIMARY.name(), workflowName));
			secondaryCluster = new CommonConfig.Cluster();
			secondaryCluster.setBootstrapServers(String.valueOf(input.getSecondaryCluster().get("bootStrapServers")));
			secondaryCluster.setAuthMechanism(AuthMechanism.valueOf(String.valueOf(input.getSecondaryCluster().get("authMechanism"))));
			secondaryCluster.setUsername(getTaskKey(taskDefName, ClusterType.TXN_HA_SECONDARY.name(), workflowName));
			secondaryCluster.setPassword(getTaskSecret(taskDefName, ClusterType.TXN_HA_SECONDARY.name(), workflowName));
		}
		ProducerAcks producerAcks = ProducerAcks.valueOf(input.getAcks());
		int retries = Math.min(input.getRetries(), 3);
		ProducerConfig producerConfig = ProducerConfig.builder().primary(primaryCluster).
				clientId(input.getClientId()).enableCompression(input.isEnableCompression()).
				acks(producerAcks).topics(topics).retries(retries).build();
		if (secondaryCluster != null) {
			producerConfig.setSecondary(secondaryCluster);
		}
		logger.info("Created producer config " + producerConfig + " successfully");
		return producerConfig;
	}

	private Topic getTopic(ConfluentKafkaPublishTask.Input input) {
		Topic topic = new Topic();
		topic.setName(String.valueOf(input.getTopic().get("name")));
		topic.setFaultStrategy(Topic.FaultStrategy.valueOf(String.valueOf(input.getTopic().get("faultStrategy"))));
		topic.setEnableEncryption(Boolean.valueOf(String.valueOf(input.getTopic().get("enableEncryption"))));
		topic.setKeyId(String.valueOf(input.getTopic().get("keyId")));
		return topic;
	}

	private String getTaskKey(String taskDefName, String clusterType, String workflowName) {
		String userName = configuration.getProperty(taskDefName.toUpperCase() +  "_" + KAFKA_PREFIX + "_" + ClusterType.valueOf(clusterType).key, "");
		if (userName != "") {
			logger.info("Key found for task" + taskDefName + " for cluster " + clusterType);
			return userName;
		}
		userName = configuration.getProperty( workflowName.toUpperCase() + "_" + taskDefName.toUpperCase() +  "_" + KAFKA_PREFIX + "_" + ClusterType.valueOf(clusterType).key, "");
		 if (userName != "") {
			 logger.info("Key found for task" + taskDefName + " in workflow " + workflowName + " for cluster " + clusterType);
			return userName;
		}
		logger.info("Key not found for task" + taskDefName + " for cluster " + clusterType);
		return "";
	}

	private String getTaskSecret(String taskDefName, String clusterType, String workflowName) {
		String password = configuration.getProperty(taskDefName.toUpperCase() +  "_" + KAFKA_PREFIX + "_" + ClusterType.valueOf(clusterType).secret , "");
		if (password != "") {
			logger.info("Secret found for task" + taskDefName + " for cluster " + clusterType);
			return password;
		}
		password = configuration.getProperty( workflowName.toUpperCase() + "_" + taskDefName.toUpperCase() +  "_" + KAFKA_PREFIX + "_" + ClusterType.valueOf(clusterType).secret, "");
		if (password != "") {
			logger.info("Secret found for task" + taskDefName + " in workflow " + workflowName + " for cluster " + clusterType);
			return password;
		}
		logger.info("Secret not found for task" + taskDefName + " for cluster " + clusterType);
		return "";
	}
}

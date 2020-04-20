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
	private static final String FLO_PREFIX = "FLO__CONFLUENT_KAFKA_PUBLISH__";
	public enum ClusterType {
		BATCH("KAFKA_BATCH_PRIMARY_API_KEY", "KAFKA_BATCH_PRIMARY_API_SECRET"),
		TXN("KAFKA_TXN_PRIMARY_API_KEY", "KAFKA_TXN_PRIMARY_API_SECRET"),
		TXN_HA_PRIMARY("KAFKA_TXN_HA_PRIMARY_API_KEY", "KAFKA_TXN_HA_PRIMARY_API_SECRET"),
		TXN_HA_SECONDARY("KAFKA_TXN_HA_SECONDARY_API_KEY", "KAFKA_TXN_HA_SECONDARY_API_SECRET");

		private String  key;

		private String secret;

		ClusterType(String key, String secret){
			this.key = key;
			this.secret = secret;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ConfluentKafkaProducerManager.class);

	public static final RemovalListener<ProducerConfig, Producer> LISTENER = notification -> {
		notification.getValue().stop();
		logger.info("Closed producer for {}",notification.getKey());
	};


	@VisibleForTesting
	final String requestTimeoutConfig;
	private Cache<ProducerConfig, Producer> kafkaProducerCache;

	public ConfluentKafkaProducerManager(Configuration configuration) {
		this.configuration = configuration;
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_PUBLISH_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		int cacheSize = configuration.getIntProperty(KAFKA_PRODUCER_CACHE_SIZE, DEFAULT_CACHE_SIZE);
		int cacheTimeInMs = configuration.getIntProperty(KAFKA_PRODUCER_CACHE_TIME_IN_MILLIS, DEFAULT_CACHE_TIME_IN_MILLIS);
		this.kafkaProducerCache = CacheBuilder.newBuilder().removalListener(LISTENER)
				.maximumSize(cacheSize).expireAfterAccess(cacheTimeInMs, TimeUnit.MILLISECONDS)
				.build();
	}


	public Producer getProducer(ConfluentKafkaPublishTask.Input input, String taskDefName) {

		ProducerConfig producerConfig = getProducerProperties(input, taskDefName);

		return getFromCache(producerConfig, () -> {
				Producer producer = new Producer(producerConfig);
				producer.start();
				return producer;
		});

	}

	@VisibleForTesting
	Producer getFromCache(ProducerConfig producerConfig, Callable<Producer> createProducerCallable) {
		try {
			return kafkaProducerCache.get(producerConfig, createProducerCallable);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@VisibleForTesting
	ProducerConfig getProducerProperties(ConfluentKafkaPublishTask.Input input, String taskDefName) {

		Map<String, Topic> topics = new HashMap<>();
		Topic topic = new Topic();
		topic.setName(String.valueOf(input.getTopic().get("name")));
		topic.setFaultStrategy(Topic.FaultStrategy.valueOf(String.valueOf(input.getTopic().get("faultStrategy"))));
		topic.setEnableEncryption(Boolean.valueOf(String.valueOf(input.getTopic().get("enableEncryption"))));
		topic.setKeyId(String.valueOf(input.getTopic().get("keyId")));
		topics.put(topic.getName(), topic);

		CommonConfig.Cluster primaryCluster =  new CommonConfig.Cluster();
		CommonConfig.Cluster secondaryCluster = null;
		primaryCluster.setBootstrapServers(String.valueOf(input.getPrimaryCluster().get("bootStrapServers")));
		primaryCluster.setAuthMechanism(AuthMechanism.valueOf(String.valueOf(input.getPrimaryCluster().get("authMechanism"))));
		if (input.getClusterType().equals(ClusterType.BATCH.name()) || input.getClusterType().equals(ClusterType.TXN.name())) {
			String userName = getTaskKey(taskDefName, input.getClusterType());
			String password = getTaskSecret(taskDefName,input.getClusterType());
			primaryCluster.setUsername(userName);
			primaryCluster.setPassword(password);
		} else {
			primaryCluster.setUsername(getTaskKey(taskDefName, ClusterType.TXN_HA_PRIMARY.name()));
			primaryCluster.setPassword(getTaskSecret(taskDefName, ClusterType.TXN_HA_PRIMARY.name()));
			secondaryCluster = new CommonConfig.Cluster();
			secondaryCluster.setBootstrapServers(String.valueOf(input.getSecondaryCluster().get("bootStrapServers")));
			secondaryCluster.setAuthMechanism(AuthMechanism.valueOf(String.valueOf(input.getSecondaryCluster().get("authMechanism"))));
			secondaryCluster.setUsername(getTaskKey(taskDefName, ClusterType.TXN_HA_SECONDARY.name()));
			secondaryCluster.setPassword(getTaskSecret(taskDefName, ClusterType.TXN_HA_SECONDARY.name()));
		}
		ProducerAcks producerAcks = ProducerAcks.valueOf(input.getAcks());
		int retries = Math.min(input.getRetries(), 3);
		ProducerConfig producerConfig = ProducerConfig.builder().primary(primaryCluster).clientId(input.getClientId()).enableCompression(input.isEnableCompression()).acks(producerAcks).topics(topics).retries(retries).build();
		if (secondaryCluster != null) {
			producerConfig.setSecondary(secondaryCluster);
		}
		logger.info("Created producer config " + producerConfig + " successfully");
		return producerConfig;
	}

	String getTaskKey(String taskDefName, String clusterType) {
		String userName = configuration.getProperty( FLO_PREFIX +  "__" + taskDefName +  "__" + ClusterType.valueOf(clusterType).key , "");
		if (userName != "") {
			return userName;
		} else if (Boolean.valueOf(configuration.getProperty( "DEFAULT_SECRET_ENABLED" , "false"))) {
			return configuration.getProperty( ClusterType.valueOf(clusterType).key, "");
		}
		return "";
	}

	String getTaskSecret(String taskDefName, String clusterType) {
		String password = configuration.getProperty(FLO_PREFIX +  "__" + taskDefName +  "__" + ClusterType.valueOf(clusterType).secret, "");
		if (password != "") {
			return password;
		} else if (Boolean.valueOf(configuration.getProperty( "DEFAULT_SECRET_ENABLED" , "false"))) {
			return configuration.getProperty( ClusterType.valueOf(clusterType).secret , "");
		}
		return "";
	}
}

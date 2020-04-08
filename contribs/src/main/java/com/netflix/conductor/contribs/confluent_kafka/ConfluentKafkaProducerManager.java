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

	private static final Logger logger = LoggerFactory.getLogger(ConfluentKafkaProducerManager.class);

	public static final RemovalListener<ProducerConfig, Producer> LISTENER = notification -> {
		notification.getValue().stop();
		logger.info("Closed producer for {}",notification.getKey());
	};


	public final String requestTimeoutConfig;
	private Cache<ProducerConfig, Producer> kafkaProducerCache;

	public ConfluentKafkaProducerManager(Configuration configuration) {
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_PUBLISH_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		int cacheSize = configuration.getIntProperty(KAFKA_PRODUCER_CACHE_SIZE, DEFAULT_CACHE_SIZE);
		int cacheTimeInMs = configuration.getIntProperty(KAFKA_PRODUCER_CACHE_TIME_IN_MILLIS, DEFAULT_CACHE_TIME_IN_MILLIS);
		this.kafkaProducerCache = CacheBuilder.newBuilder().removalListener(LISTENER)
				.maximumSize(cacheSize).expireAfterAccess(cacheTimeInMs, TimeUnit.MILLISECONDS)
				.build();
	}


	public Producer getProducer(ConfluentKafkaPublishTask.Input input) {

		ProducerConfig producerConfig = getProducerProperties(input);

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
	ProducerConfig getProducerProperties(ConfluentKafkaPublishTask.Input input) {

		Map<String, Topic> topics = new HashMap<>();
		Topic topic = new Topic();
		topic.setName(String.valueOf(input.getTopic().get("name")));
		topic.setFaultStrategy(Topic.FaultStrategy.valueOf(String.valueOf(input.getTopic().get("faultStrategy"))));
		topic.setEnableEncryption(Boolean.valueOf(String.valueOf(input.getTopic().get("enableEncryption"))));
		topic.setKeyId(String.valueOf(input.getTopic().get("keyId")));
		topics.put(topic.getName(), topic);

		CommonConfig.Cluster primaryCluster =  new CommonConfig.Cluster();
		primaryCluster.setBootstrapServers(String.valueOf(input.getPrimaryCluster().get("bootStrapServers")));
		primaryCluster.setAuthMechanism(AuthMechanism.valueOf(String.valueOf(input.getPrimaryCluster().get("authMechanism"))));
		primaryCluster.setUsername(String.valueOf(input.getPrimaryCluster().get("username")));
		primaryCluster.setPassword(String.valueOf(input.getPrimaryCluster().get("password")));

		CommonConfig.Cluster secondaryCluster =  new CommonConfig.Cluster();
		secondaryCluster.setBootstrapServers(String.valueOf(input.getSecondaryCluster().get("bootStrapServers")));
		secondaryCluster.setAuthMechanism(AuthMechanism.valueOf(String.valueOf(input.getSecondaryCluster().get("authMechanism"))));
		secondaryCluster.setUsername(String.valueOf(input.getSecondaryCluster().get("username")));
		secondaryCluster.setPassword(String.valueOf(input.getSecondaryCluster().get("password")));
		ProducerAcks producerAcks = ProducerAcks.valueOf(input.getAcks());
		int retries = Math.min(input.getRetries(), 3);
		ProducerConfig producerConfig = ProducerConfig.builder().primary(primaryCluster).secondary(secondaryCluster).clientId(input.getClientId()).enableCompression(input.isEnableCompression()).acks(producerAcks).topics(topics).retries(retries).build();
		logger.info("Created producer config " + producerConfig + " successfully");
		return producerConfig;
	}
}

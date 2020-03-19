package com.netflix.conductor.kafka.index.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.data.Record;
import com.netflix.conductor.kafka.index.constants.ProducerConstants;
import com.netflix.conductor.kafka.index.mapper.MapperFactory;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Properties;

@Trace
@Singleton
public class KafkaProducer implements ProducerDAO {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private String topic;
	private ObjectMapper om = MapperFactory.getObjectMapper();

	private int requestTimeoutConfig;
	private int maxBlockTimeMs;
	private Producer producer;

	@Inject
	public KafkaProducer(Configuration configuration) {
		this.topic = configuration.getProperty(ProducerConstants.KAFKA_PRODUCER_TOPIC, ProducerConstants.PRODUCER_DEFAULT_TOPIC);
		this.requestTimeoutConfig = configuration.getIntProperty(ProducerConstants.KAFKA_REQUEST_TIMEOUT_MS, ProducerConstants.DEFAULT_REQUEST_TIMEOUT);
		this.maxBlockTimeMs = configuration.getIntProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, ProducerConstants.DEFAULT_REQUEST_BLOCK_TIMEOUT);
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConstants.STRING_SERIALIZER);
		producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, this.maxBlockTimeMs);

		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutConfig);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerConstants.STRING_SERIALIZER);
		producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerConfig);

		SimpleModule serializeModule = new SimpleModule();
		om.registerModule(serializeModule);
	}

	@Override
	public void send(String operationType, String documentType, Object value) {
		try {
			Record d = new Record(operationType, documentType, value);
			ProducerRecord rec = new ProducerRecord(this.topic, om.writeValueAsString(d));
			producer.send(rec);
		} catch (Exception e) {
			logger.error("Failed to publish to kafka - unknown exception:", e);
			Monitors.recordKafkaPublishError();
		}
	}
}


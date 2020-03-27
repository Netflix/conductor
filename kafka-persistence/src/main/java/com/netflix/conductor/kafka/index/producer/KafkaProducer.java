package com.netflix.conductor.kafka.index.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.data.Record;
import com.netflix.conductor.kafka.index.configuration.KafkaConfiguration;
import com.netflix.conductor.kafka.index.mapper.MapperFactory;
import com.netflix.conductor.metrics.Monitors;
import com.swiggy.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

@Trace
@Singleton
public class KafkaProducer implements ProducerDAO {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private String topic ="ff-flo-event-logs";
	private ObjectMapper om = MapperFactory.getObjectMapper();
	private Producer producer;

	@Inject
	public KafkaProducer(Configuration configuration, KafkaConfiguration kafkaConfiguration) {
		this.topic = configuration.getProperty("flo.indexer.topic.name", this.topic);

		SimpleModule serializeModule = new SimpleModule();
		om.registerModule(serializeModule);
		this.producer = kafkaConfiguration.getTier1Producer();
		this.producer.start();
		logger.info("Kafka Producer Started successfully");
	}

	@Override
	public void send(String operationType, String documentType, Object value) {
		try {
			Record d = new Record(operationType, documentType, value);
			producer.send(this.topic, om.writeValueAsString(d));
		} catch (Exception e) {
			logger.error("Failed to publish to kafka - unknown exception:", e);
			Monitors.recordKafkaPublishError();
		}
	}

	@PreDestroy
	public void close() {
		if (this.producer != null) {
			logger.info("Stopping the kafka producer");
			this.producer.stop();
		}
	}
}


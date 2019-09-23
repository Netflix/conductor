package com.netflix.conductor.dao.kafka.index.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ProducerDAO;
import com.netflix.conductor.dao.kafka.index.constants.ProducerConstants;
import com.netflix.conductor.dao.kafka.index.mapper.Mapper;
import com.netflix.conductor.dao.kafka.index.serialiser.DataSerializer;
import com.netflix.conductor.dao.kafka.index.serialiser.Record;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Trace
@Singleton
public class KafkaProducer implements ProducerDAO {

	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private String topic;
	private ObjectMapper om = Mapper.getObjectMapper();

	private String requestTimeoutConfig;
	private Producer producer;

	@Override
	public void init(Configuration configuration) {
		this.topic = configuration.getProperty(ProducerConstants.KAFKA_PRODUCER_TOPIC, ProducerConstants.PRODUCER_DEFAULT_TOPIC);
		this.requestTimeoutConfig = configuration.getProperty(ProducerConstants.KAFKA_REQUEST_TIMEOUT_MS, ProducerConstants.DEFAULT_REQUEST_TIMEOUT);
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerConstants.DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConstants.STRING_SERIALIZER);
		String requestTimeoutMs = requestTimeoutConfig;
		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerConstants.STRING_SERIALIZER);
		producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerConfig);

		SimpleModule serializeModule = new SimpleModule();
		serializeModule.addSerializer(Record.class, new DataSerializer());
		om.registerModule(serializeModule);

	}

	@Override
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
    public void close() {
	    producer.close();
    }

}


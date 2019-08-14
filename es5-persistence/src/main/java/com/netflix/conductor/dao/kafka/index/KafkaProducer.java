package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.KafkaProducerDAO;
import com.netflix.conductor.metrics.Monitors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Trace
@Singleton
public class KafkaProducer implements KafkaProducerDAO {

	public static final String KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String KAFKA_PRODUCER_TOPIC = "kafka.consumer.topic";
	public static final String PRODUCER_DEFAULT_TOPIC = "test";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";
	public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private final String topic;
	private ObjectMapper om = Record.objectMapper();

	public final String requestTimeoutConfig;
	private Producer producer;

	@Inject
	public KafkaProducer(Configuration configuration) {
		this.topic = configuration.getProperty(KAFKA_PRODUCER_TOPIC, PRODUCER_DEFAULT_TOPIC);
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
		String requestTimeoutMs = requestTimeoutConfig;
		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
		producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerConfig);

		SimpleModule serializeModule = new SimpleModule();
		serializeModule.addSerializer(Record.class, new DataSerializer());
		om.registerModule(serializeModule);

	}

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

}

class DataSerializer extends StdSerializer<Record> {

	public DataSerializer() {
		this(null);
	}

	public DataSerializer(Class<Record> t) {
		super(t);
	}

	@Override
	public void serialize(
			Record value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException {

		jgen.writeStartObject();
		jgen.writeStringField("type", value.type);
		jgen.writeObjectField("payload", value.payload);
		jgen.writeEndObject();
	}

}

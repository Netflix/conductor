package com.netflix.conductor.dao.kafka.index;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.core.config.Configuration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;

@Trace
@Singleton
public class KafkaPublishManager {

	public static final String KAFKA_PUBLISH_REQUEST_TIMEOUT_MS = "kafka.publish.request.timeout.ms";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";
	public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	private static final Logger logger = LoggerFactory.getLogger(KafkaPublishManager.class);
	private ObjectMapper om = objectMapper();

	private static ObjectMapper objectMapper() {
		final ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		return om;
	}

	public final String requestTimeoutConfig;
	private Producer producer;
	private Consumer consumer;

	@Inject
	public KafkaPublishManager(Configuration configuration) {
		this.requestTimeoutConfig = configuration.getProperty(KAFKA_PUBLISH_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
		String requestTimeoutMs = requestTimeoutConfig;
		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
		producer = new KafkaProducer<String, String>(producerConfig);

		Properties consumerConfig = new Properties();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG));
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
		consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
		consumerConfig.put("group.id",  "_group");
		consumer = new KafkaConsumer<String, String>(consumerConfig);
		consumer.subscribe(Arrays.asList("test"));

		Executors.newFixedThreadPool(1).submit(()->consume());

		SimpleModule serializeModule = new SimpleModule();
		serializeModule.addSerializer(Data.class, new DataSerializer());
		om.registerModule(serializeModule);

		SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(Data.class, new DataDeSerializer());
		om.registerModule(deserializeModule);

	}

	private void consume() {
		while(true) {
			try {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3, 1));

				logger.info("polled {} messages from kafka topic.", records.count());
				records.forEach(record -> {
					logger.debug(
							"Consumer Record: " + "key: {}, " + "value: {}, " + "partition: {}, " + "offset: {}",
							record.key(), record.value(), record.partition(), record.offset());
					try {
						Data d = om.readValue(record.value(), Data.class);
						switch (d.t) {
							case "workflow":

								break;
							case "task":
								break;
							case "task_log":
								break;
							case "event":
								break;
							case "message":
								break;
							default:
								break;
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			} catch (KafkaException e) {
				logger.error("kafka consumer message polling failed.", e);
			}
		}
	}

	public void send(String t, Object value) {
		try {
			Data d = new Data(t, value);
			ProducerRecord rec = new ProducerRecord("test", om.writeValueAsString(d));
			Future send = producer.send(rec);
			try {
				send.get();
			} catch (ExecutionException ec) {
				logger.error("Failed to invoke kafka task - execution exception {}", ec);
			}
		} catch (Exception e) {
			logger.error("Failed to publish to kafka - unknown exception:", e);
		}
	}

}

class DataSerializer extends StdSerializer<Data> {

	public DataSerializer() {
		this(null);
	}

	public DataSerializer(Class<Data> t) {
		super(t);
	}

	@Override
	public void serialize(
			Data value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException {

		jgen.writeStartObject();
		jgen.writeStringField("type", value.t);
		jgen.writeObjectField("data", value.data);
		jgen.writeEndObject();
	}

}


class Data {
	String t;
	Object data;

	Data(String t, Object data) {
		this.t = t;
		this.data = data;
	}
}

class DataDeSerializer extends StdDeserializer<Data> {

	public DataDeSerializer() {
		this(null);
	}

	@Override
	public Data deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		JsonNode node = jp.getCodec().readTree(jp);
		String type = node.get("type").toString();
		Object data = node.get("data");

		return new Data(type, data);
	}

	public DataDeSerializer(Class<Data> t) {
		super(t);
	}
}

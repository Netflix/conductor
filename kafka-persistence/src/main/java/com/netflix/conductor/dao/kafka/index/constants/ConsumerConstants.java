package com.netflix.conductor.dao.kafka.index.constants;

public class ConsumerConstants {
    public static final String KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
    public static final String KAFKA_CONSUMER_POLL_INTERVAL = "kafka.KafkaConsumer.poll.interval.ms";
    public static final String KAFKA_CONSUMER_TOPIC = "kafka.KafkaConsumer.topic";
    public static final String CONSUMER_DEFAULT_TOPIC = "mytest";
    public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String DEFAULT_REQUEST_TIMEOUT = "1000";
    public static final int CONSUMER_DEFAULT_POLL_INTERVAL = 1;
    public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
}
package com.netflix.conductor.dao.kafka.index.constants;

public class ProducerConstants {

    public static final String KAFKA_REQUEST_TIMEOUT_MS = "kafka.request.timeout.ms";
    public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KAFKA_PRODUCER_TOPIC = "kafka.producer.topic";
    public static final String PRODUCER_DEFAULT_TOPIC = "mytest";
    public static final int DEFAULT_REQUEST_TIMEOUT = 100;
    public static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static final int DEFAULT_REQUEST_BLOCK_TIMEOUT = 100;
}

/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.netflix.conductor.contribs.queue.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;
import rx.Observable;
import rx.Observable.OnSubscribe;

/**
 * Reads the properties with prefix 'kafka.producer.', 'kafka.consumer.' and 'kafka.' from the
 * provided configuration. Initializes a producer and consumer based on the given value. Queue name
 * is driven from the workflow. It is assumed that the queue name provided is already configured in
 * the kafka cluster.
 * 
 * @author preeth
 *
 */
public class KafkaObservableQueue implements ObservableQueue {

  private static final Logger logger = LoggerFactory.getLogger(KafkaObservableQueue.class);

  private static final String QUEUE_TYPE = "kafka";

  private static final String KAFKA_PRODUCER_PREFIX = "kafka.producer.";

  private static final String KAFKA_CONSUMER_PREFIX = "kafka.consumer.";

  private static final String KAFKA_PREFIX = "kafka.";

  private final String queueName;

  private final int pollIntervalInMS;

  private final int pollTimeoutInMs;

  private KafkaProducer<String, String> producer;

  private KafkaConsumer<String, String> consumer;

  @Inject
  public KafkaObservableQueue(String queueName, Configuration config) {

    this.queueName = queueName;
    this.pollIntervalInMS = config.getIntProperty("kafka.consumer.pollingInterval", 1000);
    this.pollTimeoutInMs = config.getIntProperty("kafka.consumer.longPollTimeout", 1000);
    // Init Kafka producer and consumer properties
    init(config);
  }

  /**
   * Initializes the kafka producer with the defaults. Fails in case of any mandatory configs are
   * missing.
   * 
   * @param config
   */
  private void init(Configuration config) {
    Properties producerProps = new Properties();
    Properties consumerProps = new Properties();
    consumerProps.put("group.id", queueName + "_group");
    String serverId = config.getServerId();
    consumerProps.put("client.id", queueName + "_consumer_" + serverId);
    producerProps.put("client.id", queueName + "_producer_" + serverId);
    Map<String, Object> configMap = config.getAll();
    if (Objects.isNull(configMap)) {
      throw new RuntimeException("Configuration missing");
    }
    for (Entry<String, Object> entry : configMap.entrySet()) {
      String key = entry.getKey();
      String value = (String) entry.getValue();
      if (key.startsWith(KAFKA_PREFIX)) {
        if (key.startsWith(KAFKA_PRODUCER_PREFIX)) {
          producerProps.put(key.replaceAll(KAFKA_PRODUCER_PREFIX, ""), value);
        } else if (key.startsWith(KAFKA_CONSUMER_PREFIX)) {
          consumerProps.put(key.replaceAll(KAFKA_CONSUMER_PREFIX, ""), value);
        } else {
          producerProps.put(key.replaceAll(KAFKA_PREFIX, ""), value);
          consumerProps.put(key.replaceAll(KAFKA_PREFIX, ""), value);
        }
      }
    }
    checkProducerProps(producerProps);
    checkConsumerProps(consumerProps);
    applyConsumerDefaults(consumerProps);
    try {
      // Init Kafka producer and consumer
      producer = new KafkaProducer<>(producerProps);
      consumer = new KafkaConsumer<>(consumerProps);
      // Assumption is that the queueName provided is already
      // configured within the Kafka cluster.
      consumer.subscribe(Collections.singletonList(queueName));
      logger.info("KafkaObservableQueue initialized for {}", queueName);

    } catch (KafkaException ke) {
      logger.error("Kafka initialization failed.", ke);
      throw new RuntimeException(ke);
    }
  }

  /**
   * Apply consumer defaults, if not configured.
   * 
   * @param consumerProps
   */
  private void applyConsumerDefaults(Properties consumerProps) {
    if (null == consumerProps.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }
    if (null == consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }
  }

  /**
   * Checks mandatory configs are available for kafka consumer.
   * 
   * @param consumerProps
   */
  private void checkConsumerProps(Properties consumerProps) {
    List<String> mandatoryKeys = Arrays.asList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    List<String> keysNotFound = hasKeyAndValue(consumerProps, mandatoryKeys);
    if (keysNotFound != null && keysNotFound.size() > 0) {
      logger.error("Configuration missing for Kafka consumer. {}" + keysNotFound.toString());
      throw new RuntimeException(
          "Configuration missing for Kafka consumer." + keysNotFound.toString());
    }
  }

  /**
   * Checks mandatory configurations are available for kafka producer.
   * 
   * @param producerProps
   */
  private void checkProducerProps(Properties producerProps) {
    List<String> mandatoryKeys = Arrays.asList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    List<String> keysNotFound = hasKeyAndValue(producerProps, mandatoryKeys);
    if (keysNotFound != null && keysNotFound.size() > 0) {
      logger.error("Configuration missing for Kafka producer. {}" + keysNotFound.toString());
      throw new RuntimeException(
          "Configuration missing for Kafka producer." + keysNotFound.toString());
    }
  }

  /**
   * Validates whether the property has given keys.
   * 
   * @param prop
   * @param keys
   * @return
   */
  private List<String> hasKeyAndValue(Properties prop, List<String> keys) {
    List<String> keysNotFound = new ArrayList<>();
    for (String key : keys) {
      if (!prop.containsKey(key) || Objects.isNull(prop.get(key))) {
        keysNotFound.add(key);
      }
    }
    return keysNotFound;

  }

  @Override
  public Observable<Message> observe() {
    OnSubscribe<Message> subscriber = getOnSubscribe();
    return Observable.create(subscriber);
  }

  @Override
  public List<String> ack(List<Message> messages) {
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    messages.forEach(message -> {
      String[] idParts = message.getId().split(":");

      currentOffsets.put(new TopicPartition(idParts[1], Integer.valueOf(idParts[2])),
          new OffsetAndMetadata(Integer.valueOf(idParts[3]) + 1, "no metadata"));
    });
    try {
      consumer.commitSync(currentOffsets);
    } catch (KafkaException ke) {
      logger.error("kafka consumer selective commit failed.", ke);
      return messages.stream().map(message -> message.getId()).collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public void setUnackTimeout(Message message, long unackTimeout) {}

  @Override
  public void publish(List<Message> messages) {
    publishMessages(messages);
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public String getType() {
    return QUEUE_TYPE;
  }

  @Override
  public String getName() {
    return queueName;
  }

  @Override
  public String getURI() {
    return queueName;
  }

  /**
   * Polls the topics and retrieve the messages.
   * 
   * @return List of messages
   */
  @VisibleForTesting()
  List<Message> receiveMessages() {
    List<Message> messages = new ArrayList<>();
    try {

      ConsumerRecords<String, String> records = consumer.poll(pollTimeoutInMs);

      if (records.count() == 0) {
        return messages;
      }

      logger.info("polled {} messages from kafka topic.", records.count());
      records.forEach(record -> {
        logger.debug(
            "Consumer Record: " + "key: {}, " + "value: {}, " + "partition: {}, " + "offset: {}",
            record.key(), record.value(), record.partition(), record.offset());
        String id =
            record.key() + ":" + record.topic() + ":" + record.partition() + ":" + record.offset();
        Message message = new Message(id, String.valueOf(record.value()), "");
        messages.add(message);
      });
    } catch (KafkaException e) {
      logger.error("kafka consumer message polling failed.", e);
    }
    return messages;
  }

  /**
   * Publish the messages to the given topic.
   * 
   * @param messages
   */
  @VisibleForTesting()
  void publishMessages(List<Message> messages) {

    if (messages == null || messages.isEmpty()) {
      return;
    }
    for (Message message : messages) {
      final ProducerRecord<String, String> record =
          new ProducerRecord<>(queueName, message.getId(), message.getPayload());

      RecordMetadata metadata;
      try {
        metadata = producer.send(record).get();
        logger.debug("Producer Record: key {}, value {}, partition {}, offset {}", record.key(),
            record.value(), metadata.partition(), metadata.offset());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Publish message to kafka topic {} failed with an error: {}", queueName,
            e.getMessage(), e);
      } catch (ExecutionException e) {
        logger.error("Publish message to kafka topic {} failed with an error: {}", queueName,
            e.getMessage(), e);
        throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR,
            "Failed to publish the event");
      }
    }
    logger.info("Messages published to kafka topic {}. count {}", queueName, messages.size());

  }

  @VisibleForTesting
  OnSubscribe<Message> getOnSubscribe() {
    return subscriber -> {
      Observable<Long> interval = Observable.interval(pollIntervalInMS, TimeUnit.MILLISECONDS);
      interval.flatMap((Long x) -> {
        List<Message> msgs = receiveMessages();
        return Observable.from(msgs);
      }).subscribe(subscriber::onNext, subscriber::onError);
    };
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.flush();
      producer.close(1000, TimeUnit.MILLISECONDS);
    }

    if (consumer != null) {
      consumer.unsubscribe();
      consumer.close(1000, TimeUnit.MILLISECONDS);
    }

  }
}

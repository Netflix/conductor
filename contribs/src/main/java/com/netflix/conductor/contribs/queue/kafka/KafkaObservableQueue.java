/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.contribs.queue.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
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
 * Reads the properties with prefix 'kafka.producer.', 'kafka.consumer.'
 * and 'kafka.' from the provided configuration. Initializes a producer 
 * and consumer based on the given value. Queue name is driven from the 
 * workflow. 
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

  private final int pollTimeInMS;

  private final int longPollTimeout;

  private final int pollCount;

  private final KafkaProducer<String, String> producer;

  private final KafkaConsumer<String, String> consumer;

  @Inject
  public KafkaObservableQueue(String queueName, Configuration config) {

    this.queueName = queueName;
    this.pollTimeInMS = config.getIntProperty("workflow.dyno.queues.pollingInterval", 100);
    this.pollCount = config.getIntProperty("workflow.dyno.queues.pollCount", 10);
    this.longPollTimeout = config.getIntProperty("workflow.dyno.queues.longPollTimeout", 1000);
    // Init Kafka producer and consumer properties
    Properties producerProps = new Properties();
    Properties consumerProps = new Properties();
    consumerProps.put("group.id", queueName + "_group");
    String serverId = config.getServerId();
    consumerProps.put("client.id", queueName + "_consumer_"+serverId);
    producerProps.put("client.id", queueName + "_producer_"+serverId);
    Map<String, Object> configMap = config.getAll();
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

    // Init Kafka producer and consumer
    producer = new KafkaProducer<>(producerProps);
    consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singletonList(queueName));
    logger.info("KafkaObservableQueue initialized for {}", queueName);
  }

  @Override
  public Observable<Message> observe() {
    OnSubscribe<Message> subscriber = getOnSubscribe();
    return Observable.create(subscriber);
  }

  @Override
  public List<String> ack(List<Message> messages) {
    return messages.stream().map(Message::getId).collect(Collectors.toList());
  }

  public void setUnackTimeout(Message message, long unackTimeout) {
  }

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

  private List<Message> receiveMessages() {
    List<Message> messages = new ArrayList<>();
    try {

      ConsumerRecords<String, String> records = consumer.poll(longPollTimeout);
      if (records.count() == 0) {
        return messages;
      }

      logger.info("polled {} messages from kafka topic.", records.count());
      records.forEach(record -> {
        logger.debug("Consumer Record: key: {}, value: {}, partition: {}, offset: {}", record.key(),
            record.value(), record.partition(), record.offset());
        Message message =
            new Message(record.key() +":"+ record.partition(), String.valueOf(record.value()), "");
        messages.add(message);
      });
      consumer.commitAsync();
    } catch (KafkaException e) {
      logger.error("kafka consumer message polling failed.", e);
    }
    return messages;
  }

  private void publishMessages(List<Message> messages) {

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
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        logger.error("Publish message to kafka topic {} failed with an error: {}", queueName,
            e.getMessage(), e);
        throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR,
            "Failed to publish the event");
      }
    }
    logger.info("Messages published to kafka topic {}. count {}", queueName, messages.size());

  }

  @VisibleForTesting
  private OnSubscribe<Message> getOnSubscribe() {
    return subscriber -> {
      Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
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

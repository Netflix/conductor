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

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.EventProcessingFailures;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.MessageEventFailure;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;

import rx.Observable;
import rx.Observable.OnSubscribe;

/**
 * Implements a Kafka event source. A Kafka consumer subscribes to Kafka topics that are dictated by
 * the 'event' attributes of Kafka event handlers. 'kafka' is the queue type, so if an event handler has an
 * 'event' attribute of 'kafka:my-topic-name', this ObservableQueue implementation will subscribe to the 'my-topic-name'
 * topic in the Kafka cluster identified by the 'kafka.events.bootstrap.servers' property (you can also set the
 * 'kafka.default.bootstrap.servers' property if the same cluster is used for all Kafka topics including the events
 * topics).
 * 
 * Kafka security is supported using JAAS. If the 'kafka.default.jaas.config.file' property is set to the location of
 * a JAAS configuration file (in the classpath), the 'java.security.auth.login.config' system property will be set to
 * that file location. That config file can specify a login module. If the login module chosen is 
 * com.netflix.conductor.contribs.kafka.KafkaLoginModule, setting the kafka.events.jaas.username (or kafka.default.jaas.username
 * if all Kafka topics use the same login) and kafka.events.jaas.password (or kafka.default.jaas.password
 * if all Kafka topics use the same password) will allow those credentials to be used to connect to the Kafka topics.  
 * 
 * If there are errors processing the events picked up from the event handler's topic and a topic was set up that has the same
 * name as the topic but with a '-errors' suffix, the error (in the form of a serialized
 * com.netflix.conductor.core.events.queue.MessageEventFailure JSON will be written to that error topic. 
 * 
 * The 'kafka.events.pollingInterval' property (or the kafka.default.pollingInterval property if all Kafka consumers use
 * the same value) can be used to specify how many milliseconds elapses before the next attempt at consuming events happens.
 * 
 * The 'kafka.events.longPollTimeout' property (or the kafka.default.longPollTimeout property if all Kafka consumers use
 * the same value) can be used to specify how many milliseconds the consumer will wait for an event to arrive.
 * 
 * @author preeth, rickfish
 *
 */
public class KafkaObservableQueue implements ObservableQueue {

	private static final Logger logger = LoggerFactory.getLogger(KafkaObservableQueue.class);

	private static final String QUEUE_TYPE = "kafka";

	private final String queueName;

	private int pollIntervalInMS;

	private final int pollTimeoutInMs;

	private KafkaProducer<String, String> producer;

	private List<KafkaConsumer<String, String>> consumers;

	private final ObjectMapper objectMapper = new ObjectMapper();

	@Inject
	public KafkaObservableQueue(String queueName, Configuration config) {
		this.queueName = queueName;
		this.pollIntervalInMS = config.getKafkaEventsPollingIntervalMS();
		this.pollTimeoutInMs = config.getKafkaEventsPollTimeoutMS();
		init(config);
	}

	/**
	 * Initializes the kafka producer with the defaults. Fails in case of any
	 * mandatory configs are missing.
	 * 
	 * @param config
	 */
	private void init(Configuration config) {
		try {
			Properties consumerProperties = new Properties();
			/**
			 * A JAAS configuration file can be specified that contains authentication instructions for the Kafka topic 
			 */
			String filename = config.getKafkaEventsJaasConfigFile();
			if(filename != null) {
				URL jaasConfigUrl = this.getClass().getResource(filename);
				System.setProperty("java.security.auth.login.config", jaasConfigUrl == null ? filename : jaasConfigUrl.toExternalForm());
			}
			String prop = config.getKafkaEventsBootstrapServers();
			if(prop != null) {
				consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop);
			}
			prop = config.getKafkaEventsConsumerGroupId();
			if(prop != null) {
				consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, prop);
			}
			prop = config.getKafkaAutoOffsetResetConfig();
			if(prop != null) {
				consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop);
			}
			consumerProperties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.name);
			consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
			checkConsumerProps(consumerProperties);

			consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, queueName + "_consumer_" + config.getServerId() + "_0");
			
			/**
			 * Create a consumer for each of the topic's partitions. Create one consumer first so that we can use it
			 * to get the partition information.
			 */
			KafkaConsumer<String, String> firstConsumer = new KafkaConsumer<String, String>(consumerProperties);
			firstConsumer.subscribe(Collections.singletonList(queueName));
			Map<String, List<PartitionInfo>> topics = firstConsumer.listTopics();
			if (topics != null && topics.get(queueName) != null) {
				List<PartitionInfo> partitions = topics.get(queueName);
				if (partitions != null && partitions.size() > 0) {
					this.consumers = new ArrayList<KafkaConsumer<String, String>>();
					this.consumers.add(firstConsumer);
					for (int i = 1; i < partitions.size(); i++) {
						consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, queueName + "_consumer_" + config.getServerId() + "_" + i);
						KafkaConsumer<String, String> newConsumer = new KafkaConsumer<String, String>(
								consumerProperties);
						newConsumer.subscribe(Collections.singletonList(queueName));
						this.consumers.add(newConsumer);
					}
				} else {
					logger.error("The topic '" + queueName + "' does not have any partitions!");
				}
			} else {
				logger.error("The topic '" + queueName + "' was not found!");
				firstConsumer.unsubscribe();
				firstConsumer.close();
			}

			/**
			 * Create a producer to put events on the topic for testing purposes or to put errors on the error topic.
			 */
			Properties producerProperties = new Properties();
			prop = config.getKafkaEventsBootstrapServers();
			if(prop != null) {
				producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop);
			}
			producerProperties.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.name);
			producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			checkProducerProps(producerProperties);
			producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, queueName + "_producer_" + config.getServerId());
			this.producer = new KafkaProducer<String, String>(producerProperties);
		} catch (KafkaException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Checks mandatory configs are available for kafka consumer.
	 * 
	 * @param consumerProps
	 */
	private void checkConsumerProps(Properties consumerProps) {
		List<String> mandatoryKeys = Arrays.asList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
		List<String> keysNotFound = hasKeyAndValue(consumerProps, mandatoryKeys);
		if (keysNotFound != null && keysNotFound.size() > 0) {
			logger.error("Configuration missing for Kafka consumer. {}" + keysNotFound.toString());
			throw new RuntimeException("Configuration missing for Kafka consumer." + keysNotFound.toString());
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
			throw new RuntimeException("Configuration missing for Kafka producer." + keysNotFound.toString());
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
		List<String> messageIds = new ArrayList<String>();
		/*
		 * For each message, get the partition number, find the consumer that subscribes to that partition
		 * and have that consumer commit the offset for that partition.
		 */
		for (Message message : messages) {
			String[] idParts = message.getId().split(":");
			int partitionNumber = Integer.valueOf(idParts[2]);
			if(this.consumers != null) {
				for (KafkaConsumer<String, String> consumer : this.consumers) {
					boolean didIt = false;
					for (PartitionInfo partition : consumer.partitionsFor(queueName)) {
						if (partitionNumber == partition.partition()) {
							Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
							currentOffsets.put(new TopicPartition(idParts[1], partitionNumber),
									new OffsetAndMetadata(Integer.valueOf(idParts[3]) + 1, "no metadata"));
							try {
								consumer.commitSync(currentOffsets);
								messageIds.add(message.getId());
							} catch (KafkaException ke) {
								logger.error("kafka consumer selective commit failed.", ke);
							}
							didIt = true;
							break;
						}
					}
					if (didIt) {
						break;
					}
				}
			}
		}
		return messageIds;
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

	/**
	 * Polls the topics and retrieve the messages for all consumers of the topic.
	 * 
	 * @return List of messages
	 */
	@VisibleForTesting()
	List<Message> receiveMessages() {
		List<Message> messages = new ArrayList<>();
		/*
		 * Accumulate messages for all consumers as if there is only one consumer
		 */
		if(this.consumers != null) {
			for (KafkaConsumer<String, String> consumer : this.consumers) {
				messages.addAll(receiveMessages(consumer));
			}
		}
		return messages;
	}

	/**
	 * Polls the topics and retrieve the messages for a consumer.
	 * 
	 * @return List of messages
	 */
	@VisibleForTesting()
	List<Message> receiveMessages(KafkaConsumer<String, String> consumer) {
		List<Message> messages = new ArrayList<>();
		try {

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutInMs));

			if (records.count() == 0) {
				return messages;
			}

			logger.info("polled {} messages from kafka topic.", records.count());
			records.forEach(record -> {
				logger.debug("Consumer Record: " + "key: {}, " + "value: {}, " + "partition: {}, " + "offset: {}",
						record.key(), record.value(), record.partition(), record.offset());
				String id = record.key() + ":" + record.topic() + ":" + record.partition() + ":" + record.offset();
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
			final ProducerRecord<String, String> record = new ProducerRecord<>(queueName, message.getId(),
					message.getPayload());

			RecordMetadata metadata;
			try {
				metadata = this.producer.send(record).get();
				logger.debug("Producer Record: key {}, value {}, partition {}, offset {}", record.key(), record.value(),
						metadata.partition(), metadata.offset());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.error("Publish message to kafka topic {} failed with an error: {}", queueName, e.getMessage(),
						e);
			} catch (ExecutionException e) {
				logger.error("Publish message to kafka topic {} failed with an error: {}", queueName, e.getMessage(),
						e);
				throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
			}
		}
		logger.info("Messages published to kafka topic {}. count {}", queueName, messages.size());

	}


	@Override
	public void processFailures(List<Message> messages, EventProcessingFailures failures) {
		processFailures(messages, failures.getTransientFailures());
		processFailures(messages, failures.getFailures());
	}

	public void processFailures(List<Message> messages, List<EventExecution> failures) {
		final String errorQueueName = this.queueName + "-errors";
		failures.forEach(failure -> {
			Optional<MessageEventFailure> messageEventFailure = getMessageEventFailure(failure, messages);
			if(messageEventFailure.isPresent()) {
				MessageEventFailure theFailure = messageEventFailure.get();
				try {
					String s = this.objectMapper.writeValueAsString(theFailure);
					boolean validErrorTopic = true;
					final ProducerRecord<String, String> record = new ProducerRecord<>(errorQueueName, theFailure.getMessage().getId(), s);
					RecordMetadata metadata;
					try {
						metadata = this.producer.send(record).get();
						logger.debug("Producer Record: key {}, value {}, partition {}, offset {}", record.key(), record.value(),
								metadata.partition(), metadata.offset());
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						logger.error("Publish message to kafka topic {} failed with an error: {}", errorQueueName, e.getMessage(),
								e);
					} catch (ExecutionException e) {
						if(e.getCause() instanceof TopicAuthorizationException) {
							validErrorTopic = false;
							/*
							 * This exception (although it is an 'authorization' exception, also means that the topic does not exist. 
							 * Teams do not have to set up an error topic if they don't want to.  
							 */
						} else {
							logger.error("Publish message to kafka topic {} failed with an error: {}", errorQueueName, e.getMessage(), e);
							throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
						}
					}
					processFailure(this.queueName, validErrorTopic ? errorQueueName : null, theFailure);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Meant to be overridden by derived classes if there is extra processing to be done on a message processing failure
	 * @param topicName the name of the topic that contained the event
	 * @param errorTopicName the name of the topic where the error was written
	 * @param failure the failure information
	 */
	protected void processFailure(String topicName, String errorTopicName, MessageEventFailure failure) {
	}
	
	private Optional<MessageEventFailure> getMessageEventFailure(EventExecution eventExecution, List<Message> messages) {
		Optional<MessageEventFailure> messageEventFailure = Optional.empty();
		Optional<Message> foundMessage = messages.stream().filter(message -> eventExecution.getMessageId().equals(message.getId())).findFirst();
		if(foundMessage.isPresent()) {
			messageEventFailure = Optional.of(new MessageEventFailure(foundMessage.get(), eventExecution));
		}
		return messageEventFailure;
	}
	
	@VisibleForTesting
	OnSubscribe<Message> getOnSubscribe() {
		if (this.consumers != null && this.consumers.size() > 1) {
			this.pollIntervalInMS *= this.consumers.size();
		}
		return subscriber -> {
			Observable<Long> interval = Observable.interval(pollIntervalInMS, TimeUnit.MILLISECONDS);
			interval.flatMap((Long x) -> {
				return Observable.from(receiveMessages());
			}).subscribe(subscriber::onNext, subscriber::onError);
		};
	}

	@Override
	public void close() {
		if (this.producer != null) {
			this.producer.flush();
			this.producer.close();
		}

		if (this.consumers != null && this.consumers.size() > 0) {
			this.consumers.forEach(consumer -> {
				consumer.unsubscribe();
				consumer.close();
			});
		}
	}
}
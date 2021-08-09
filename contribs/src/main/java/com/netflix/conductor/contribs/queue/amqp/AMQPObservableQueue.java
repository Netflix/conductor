/**
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.amqp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import rx.Observable;

/**
 * @author Ritu Parathody
 * 
 */
public class AMQPObservableQueue implements ObservableQueue {
	private static Logger logger = LoggerFactory.getLogger(AMQPObservableQueue.class);

	private final AMQPSettings settings;
	private final int batchSize;
	private boolean useExchange;
	private int pollTimeInMS;

	private AMQPConnection amqpConnection = null;
	
	
	
	
	protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();

	AMQPObservableQueue(final ConnectionFactory factory, final Address[] addresses, final boolean useExchange,
			final AMQPSettings settings, final int batchSize, final int pollTimeInMS) {
		if (factory == null) {
			throw new IllegalArgumentException("Connection factory is undefined");
		}
		if (addresses == null || addresses.length == 0) {
			throw new IllegalArgumentException("Addresses are undefined");
		}
		if (settings == null) {
			throw new IllegalArgumentException("Settings are undefined");
		}
		if (batchSize <= 0) {
			throw new IllegalArgumentException("Batch size must be greater than 0");
		}
		if (pollTimeInMS <= 0) {
			throw new IllegalArgumentException("Poll time must be greater than 0 ms");
		}
		amqpConnection = AMQPConnection.getInstance(factory, addresses);		
		this.useExchange = useExchange;
		this.settings = settings;
		this.batchSize = batchSize;
		this.setPollTimeInMS(pollTimeInMS);
	}



	@Override
	public Observable<Message> observe() {
		receiveMessages();
		Observable.OnSubscribe<Message> onSubscribe = subscriber -> {
			Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
			interval.flatMap((Long x) -> {
				List<Message> available = new LinkedList<>();
				messages.drainTo(available);

				if (!available.isEmpty()) {
					AtomicInteger count = new AtomicInteger(0);
					StringBuilder buffer = new StringBuilder();
					available.forEach(msg -> {
						buffer.append(msg.getId()).append("=").append(msg.getPayload());
						count.incrementAndGet();

						if (count.get() < available.size()) {
							buffer.append(",");
						}
					});

					logger.info(String.format("Batch from %s to conductor is %s", settings.getQueueOrExchangeName(),
							buffer.toString()));
				}

				return Observable.from(available);
			}).subscribe(subscriber::onNext, subscriber::onError);
		};
		return Observable.create(onSubscribe);
	}

	@Override
	public String getType() {
		return useExchange ? AMQPConstants.AMQP_EXCHANGE_TYPE : AMQPConstants.AMQP_QUEUE_TYPE;
	}

	@Override
	public String getName() {
		return settings.getEventName();
	}

	@Override
	public String getURI() {
		return settings.getQueueOrExchangeName();
	}

	public int getBatchSize() {
		return batchSize;
	}

	public AMQPSettings getSettings() {
		return settings;
	}

	public Address[] getAddresses() {
		return amqpConnection.getAddresses();
	}

	@Override
	public List<String> ack(List<Message> messages) {
		final List<String> processedDeliveryTags = new ArrayList<>();
		for (final Message message : messages) {
			try {
				logger.info("ACK message with delivery tag {}", message.getReceipt());
				amqpConnection.getOrCreateChannel(ConnectionType.SUBSCRIBER, getSettings().getQueueOrExchangeName()).basicAck(Long.valueOf(message.getReceipt()), false);
				// Message ACKed
				processedDeliveryTags.add(message.getReceipt());
			} catch (final IOException e) {
				logger.error("Cannot ACK message with delivery tag {}", message.getReceipt(), e);
			}
		}
		return processedDeliveryTags;
	}

	private static AMQP.BasicProperties buildBasicProperties(final Message message, final AMQPSettings settings) {
		return new AMQP.BasicProperties.Builder()
				.messageId(StringUtils.isEmpty(message.getId()) ? UUID.randomUUID().toString() : message.getId())
				.correlationId(
						StringUtils.isEmpty(message.getReceipt()) ? UUID.randomUUID().toString() : message.getReceipt())
				.contentType(settings.getContentType()).contentEncoding(settings.getContentEncoding())
				.deliveryMode(settings.getDeliveryMode()).build();
	}

	private void publishMessage(Message message, String exchange, String routingKey) {
		try {
			final String payload = message.getPayload();
			amqpConnection.getOrCreateChannel(ConnectionType.PUBLISHER,getSettings().getQueueOrExchangeName()).basicPublish(exchange, routingKey, buildBasicProperties(message, settings),
					payload.getBytes(settings.getContentEncoding()));
			logger.info(String.format("Published message to %s: %s", exchange, payload));
		} catch (Exception ex) {
			logger.error("Failed to publish message {} to {}", message.getPayload(), exchange, ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void publish(List<Message> messages) {
		try {
			final String exchange, routingKey;
			if (useExchange) {
				// Use exchange + routing key for publishing
				getOrCreateExchange(ConnectionType.PUBLISHER,settings.getQueueOrExchangeName(), settings.getExchangeType(), settings.isDurable(),
						settings.autoDelete(), settings.getArguments());
				exchange = settings.getQueueOrExchangeName();
				routingKey = settings.getRoutingKey();
			} else {
				// Use queue for publishing
				final AMQP.Queue.DeclareOk declareOk = getOrCreateQueue(ConnectionType.PUBLISHER,settings.getQueueOrExchangeName(),
						settings.isDurable(), settings.isExclusive(), settings.autoDelete(), settings.getArguments());
				exchange = StringUtils.EMPTY; // Empty exchange name for queue
				routingKey = declareOk.getQueue(); // Routing name is the name of queue
			}
			messages.forEach(message -> publishMessage(message, exchange, routingKey));
		} catch (final RuntimeException ex) {
			throw ex;
		} catch (final Exception ex) {
			logger.error("Failed to publish messages: {}", ex.getMessage(), ex);
			throw new RuntimeException(ex);
		}

	}

	@Override
	public void setUnackTimeout(Message message, long unackTimeout) {
		throw new UnsupportedOperationException();

	}

	@Override
	public long size() {
		try {
			return amqpConnection.getOrCreateChannel(ConnectionType.SUBSCRIBER,getSettings().getQueueOrExchangeName()).messageCount(settings.getQueueOrExchangeName());
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		amqpConnection.close();
		
	}

	public static class Builder {

		private Address[] addresses;

		private int batchSize;
		private int pollTimeInMS;

		private ConnectionFactory factory;
		private Configuration config;

		public Builder(Configuration config) {
			this.config = config;
			this.addresses = buildAddressesFromHosts();
			this.factory = buildConnectionFactory();
			/* messages polling settings */
			this.batchSize = config.getIntProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_BATCH_SIZE),
					AMQPConstants.DEFAULT_BATCH_SIZE);
			this.pollTimeInMS = config.getIntProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_POLL_TIME_IN_MS),
					AMQPConstants.DEFAULT_POLL_TIME_MS);
		}

		private Address[] buildAddressesFromHosts() {
			// Read hosts from config
			final String hosts = config.getProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_HOSTS),
					ConnectionFactory.DEFAULT_HOST);
			if (StringUtils.isEmpty(hosts)) {
				throw new IllegalArgumentException("Hosts are undefined");
			}
			return Address.parseAddresses(hosts);
		}

		private ConnectionFactory buildConnectionFactory() {
			final ConnectionFactory factory = new ConnectionFactory();
			// Get rabbitmq username from config
			final String username = config.getProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_USERNAME),
					ConnectionFactory.DEFAULT_USER);
			if (StringUtils.isEmpty(username)) {
				throw new IllegalArgumentException("Username is null or empty");
			} else {
				factory.setUsername(username);
			}
			// Get rabbitmq password from config
			final String password = config.getProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_PASSWORD),
					ConnectionFactory.DEFAULT_PASS);
			if (StringUtils.isEmpty(password)) {
				throw new IllegalArgumentException("Password is null or empty");
			} else {
				factory.setPassword(password);
			}
			// Get vHost from config
			final String virtualHost = config.getProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_VIRTUAL_HOST),
					ConnectionFactory.DEFAULT_VHOST);
			if (StringUtils.isEmpty(virtualHost)) {
				throw new IllegalArgumentException("Virtual host is null or empty");
			} else {
				factory.setVirtualHost(virtualHost);
			}
			// Get server port from config
			final int port = config.getIntProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_PORT),
					AMQP.PROTOCOL.PORT);
			if (port <= 0) {
				throw new IllegalArgumentException("Port must be greater than 0");
			} else {
				factory.setPort(port);
			}
			// Get connection timeout from config
			final int connectionTimeout = config.getIntProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_CONNECTION_TIMEOUT),
					ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT);
			if (connectionTimeout <= 0) {
				throw new IllegalArgumentException("Connection timeout must be greater than 0");
			} else {
				factory.setConnectionTimeout(connectionTimeout);
			}
			final boolean useNio = config.getBoolProperty(
					String.format(AMQPConstants.PROPERTY_KEY_TEMPLATE, AMQPConfigurations.PROPERTY_USE_NIO), false);
			if (useNio) {
				factory.useNio();
			}
			return factory;
		}

		public AMQPObservableQueue build(final boolean useExchange, final String queueURI) {
			final AMQPSettings settings = new AMQPSettings(config).fromURI(queueURI);
			return new AMQPObservableQueue(factory, addresses, useExchange, settings, batchSize, pollTimeInMS);
		}
	}

	
		

	private AMQP.Exchange.DeclareOk getOrCreateExchange(ConnectionType connectionType) throws IOException {
		return getOrCreateExchange(connectionType, settings.getQueueOrExchangeName(), settings.getExchangeType(), settings.isDurable(),
				settings.autoDelete(), settings.getArguments());
	}

	private AMQP.Exchange.DeclareOk getOrCreateExchange(ConnectionType connectionType,final String name, final String type, final boolean isDurable,
			final boolean autoDelete, final Map<String, Object> arguments) throws IOException {
		if (StringUtils.isEmpty(name)) {
			throw new RuntimeException("Exchange name is undefined");
		}
		if (StringUtils.isEmpty(type)) {
			throw new RuntimeException("Exchange type is undefined");
		}
		
		AMQP.Exchange.DeclareOk declareOk;
		try {
			declareOk = amqpConnection.getOrCreateChannel(connectionType,getSettings().getQueueOrExchangeName()).exchangeDeclare(name, type, isDurable, autoDelete, arguments);
		} catch (final IOException e) {
			logger.error("Failed to create Exchange {} of type {} ", name, type);
			throw e;
			
		}
		return declareOk;
	}

	private AMQP.Queue.DeclareOk getOrCreateQueue(ConnectionType connectionType) throws IOException {
		return getOrCreateQueue(connectionType, settings.getQueueOrExchangeName(), settings.isDurable(), settings.isExclusive(),
				settings.autoDelete(), settings.getArguments());
	}

	private AMQP.Queue.DeclareOk getOrCreateQueue(ConnectionType connectionType,final String name, final boolean isDurable, final boolean isExclusive,
			final boolean autoDelete, final Map<String, Object> arguments) throws IOException {
		if (StringUtils.isEmpty(name)) {
			throw new RuntimeException("Queue name is undefined");
		}
		
		AMQP.Queue.DeclareOk declareOk;
		try {
			declareOk = amqpConnection.getOrCreateChannel(connectionType,getSettings().getQueueOrExchangeName()).queueDeclare(name, isDurable, isExclusive, autoDelete, arguments);
		} catch (final IOException e) {
			logger.warn("Failed to create queue {}", name);
			throw e;
		}
		return declareOk;
	}



	private static Message asMessage(AMQPSettings settings, GetResponse response) throws Exception {
		if (response == null) {
			return null;
		}
		final Message message = new Message();
		message.setId(response.getProps().getMessageId());
		message.setPayload(new String(response.getBody(), settings.getContentEncoding()));
		message.setReceipt(String.valueOf(response.getEnvelope().getDeliveryTag()));
		return message;
	}

	private void receiveMessagesFromQueue(String queueName) throws Exception {
		
		Consumer consumer = new DefaultConsumer(amqpConnection.getOrCreateChannel(ConnectionType.SUBSCRIBER,getSettings().getQueueOrExchangeName())) {

			@Override
			public void handleDelivery(final String consumerTag, final Envelope envelope,
					final AMQP.BasicProperties properties, final byte[] body) throws IOException {
				try {
					Message message = asMessage(settings,
							new GetResponse(envelope, properties, body, Integer.MAX_VALUE));
					if (message != null) {
						if (logger.isDebugEnabled()) {
							logger.debug("Got message with ID {} and receipt {}", message.getId(),
									message.getReceipt());
						}
						messages.add(message);
						logger.info("receiveMessagesFromQueue- End method {}", messages);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {

				}
			}			
			
			public void handleCancel(String consumerTag) throws IOException{
				logger.error("Recieved a consumer cancel notification for subscriber. Will monitor and make changes");										
			}						
			
		};
		

		amqpConnection.getOrCreateChannel(ConnectionType.SUBSCRIBER,getSettings().getQueueOrExchangeName()).basicConsume(queueName, false, consumer);
		Monitors.recordEventQueueMessagesProcessed(getType(), queueName, messages.size());
		return;
	}

	protected void receiveMessages() {
		try {
			amqpConnection.getOrCreateChannel(ConnectionType.SUBSCRIBER,getSettings().getQueueOrExchangeName()).basicQos(batchSize);
			String queueName;
			if (useExchange) {
				// Consume messages from an exchange
				getOrCreateExchange(ConnectionType.SUBSCRIBER);
				/**
				 * Create queue if not present based on the settings provided in the queue URI or configuration properties.
				 * Sample URI format: amqp-exchange:myExchange?exchangeType=topic&routingKey=myRoutingKey&exclusive=false&autoDelete=false&durable=true
				 * Default settings if not provided in the queue URI or properties: isDurable: true, autoDelete: false, isExclusive: false
				 * The same settings are currently used during creation of exchange as well as queue.
				 * TODO: This can be enhanced further to get the settings separately for exchange and queue from the URI
				*/
				final AMQP.Queue.DeclareOk declareOk = getOrCreateQueue(ConnectionType.SUBSCRIBER,
						String.format("bound_to_%s", settings.getQueueOrExchangeName()), settings.isDurable(), settings.isExclusive(), settings.autoDelete(),
						Maps.newHashMap());
				// Bind the declared queue to exchange
				queueName = declareOk.getQueue();
				amqpConnection.getOrCreateChannel(ConnectionType.SUBSCRIBER, getSettings().getQueueOrExchangeName()).queueBind(queueName, settings.getQueueOrExchangeName(), settings.getRoutingKey());
			} else {
				// Consume messages from a queue
				queueName = getOrCreateQueue(ConnectionType.SUBSCRIBER).getQueue();
			}
			// Consume messages
			logger.info("Consuming from queue {}", queueName);
			receiveMessagesFromQueue(queueName);
		} catch (Exception exception) {
			logger.error("Exception while getting messages from RabbitMQ", exception);
			Monitors.recordObservableQMessageReceivedErrors(getType());
		}
		return;
	}

	public int getPollTimeInMS() {
		return pollTimeInMS;
	}

	public void setPollTimeInMS(int pollTimeInMS) {
		this.pollTimeInMS = pollTimeInMS;
	}

}

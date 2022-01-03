/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.amqp;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.contribs.queue.amqp.util.ConnectionType;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class AMQPConnection {

	private static Logger LOGGER = LoggerFactory.getLogger(AMQPConnection.class);
	private volatile Connection publisherConnection = null;
	private volatile Connection subscriberConnection = null;
	private ConnectionFactory factory = null;
	private Address[] addresses = null;
	private static AMQPConnection amqpConnection = null;
	private final static String PUBLISHER = "Publisher";
	private final static String SUBSCRIBER = "Subscriber";
	private static final Map<ConnectionType, Set<Channel>> RMQ_CHANNEL_MAP = new ConcurrentHashMap<ConnectionType, Set<Channel>>();
	private static final Map<String, Channel> SUB_CHANNEL_MAP = new ConcurrentHashMap<String, Channel>();

	private AMQPConnection() {

	}

	private AMQPConnection(final ConnectionFactory factory, final Address[] address) {
		this.factory = factory;
		this.addresses = address;
	}

	public static synchronized AMQPConnection getInstance(final ConnectionFactory factory, final Address[] address) {
		if (AMQPConnection.amqpConnection == null) {
			AMQPConnection.amqpConnection = new AMQPConnection(factory, address);
		}

		return AMQPConnection.amqpConnection;
	}

	// Exposed for UT
	public static void setAMQPConnection(AMQPConnection amqpConnection) {
		AMQPConnection.amqpConnection = amqpConnection;
	}

	public Address[] getAddresses() {
		return addresses;
	}

	private Connection createConnection(String connectionPrefix) {

		try {
			Connection connection = factory.newConnection(addresses,
					System.getenv("HOSTNAME") + "-" + connectionPrefix);
			if (connection == null || !connection.isOpen()) {
				throw new RuntimeException("Failed to open connection");
			}
			connection.addShutdownListener(new ShutdownListener() {
				@Override
				public void shutdownCompleted(ShutdownSignalException cause) {
					LOGGER.error("Received a shutdown exception for the connection {}. reason {} cause{}",
							connection.getClientProvidedName(), cause.getMessage(), cause);

				}
			});

			connection.addBlockedListener(new BlockedListener() {
				@Override
				public void handleUnblocked() throws IOException {
					LOGGER.info("Connection {} is unblocked", connection.getClientProvidedName());
				}

				@Override
				public void handleBlocked(String reason) throws IOException {
					LOGGER.error("Connection {} is blocked. reason: {}", connection.getClientProvidedName(), reason);
				}
			});

			return connection;
		} catch (final IOException e) {
			final String error = "IO error while connecting to "
					+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(","));
			LOGGER.error(error, e);
			throw new RuntimeException(error, e);
		} catch (final TimeoutException e) {
			final String error = "Timeout while connecting to "
					+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(","));
			LOGGER.error(error, e);
			throw new RuntimeException(error, e);
		}
	}

	public Channel getOrCreateChannel(ConnectionType connectionType, String queueOrExchangeName) throws Exception {
		LOGGER.debug("Accessing the channel for queueOrExchange {} with type {} ", queueOrExchangeName, connectionType);
		switch (connectionType) {
		case SUBSCRIBER:
			String subChnName = connectionType + ";" + queueOrExchangeName;
			if (SUB_CHANNEL_MAP.containsKey(subChnName)) {
				Channel locChn = SUB_CHANNEL_MAP.get(subChnName);
				if (locChn != null && locChn.isOpen()) {
					return locChn;
				}
			}
			synchronized (this) {
				if (subscriberConnection == null) {
					subscriberConnection = createConnection(SUBSCRIBER);
				}
			}
			Channel subChn = borrowChannel(connectionType, subscriberConnection);
			SUB_CHANNEL_MAP.put(subChnName, subChn);
			return subChn;
		case PUBLISHER:
			synchronized (this) {
				if (publisherConnection == null) {
					publisherConnection = createConnection(PUBLISHER);
				}
			}
			return borrowChannel(connectionType, publisherConnection);
		default:
			return null;
		}
	}

	private Channel getOrCreateChannel(ConnectionType connType, Connection rmqConnection) {
		// Channel creation is required
		Channel locChn = null;
		try {
			LOGGER.debug("Creating a channel for " + connType);
			locChn = rmqConnection.createChannel();
			locChn.addShutdownListener(cause -> {
				LOGGER.error(connType + " Channel has been shutdown: {}", cause.getMessage(), cause);
			});
			if (locChn == null || !locChn.isOpen()) {
				throw new RuntimeException("Fail to open " + connType + " channel");
			}
		} catch (final IOException e) {
			throw new RuntimeException("Cannot open " + connType + " channel on "
					+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(",")), e);
		}
		return locChn;
	}

	public void close() {
		LOGGER.info("Closing all connections and channels");
		try {
			closeChannelInMap(ConnectionType.PUBLISHER);
			closeChannelInMap(ConnectionType.SUBSCRIBER);
			closeConnection(publisherConnection);
			closeConnection(subscriberConnection);
		} finally {
			RMQ_CHANNEL_MAP.clear();
			publisherConnection = null;
			subscriberConnection = null;
		}
	}

	private void closeChannelInMap(ConnectionType conType) {
		Set<Channel> channels = RMQ_CHANNEL_MAP.get(conType);
		if (channels != null && !channels.isEmpty()) {
			Iterator<Channel> itr = channels.iterator();
			while (itr.hasNext()) {
				Channel channel = itr.next();
				closeChannel(channel);
			}
			channels.clear();

		}

	}

	private void closeConnection(Connection connection) {
		if (connection == null) {
			LOGGER.warn("Connection is null. Do not close it");
		} else {
			try {
				connection.close();
			} catch (Exception e) {
				LOGGER.warn("Fail to close connection: {}", e.getMessage(), e);
			}
		}
	}

	private void closeChannel(Channel channel) {
		if (channel == null) {
			LOGGER.warn("Channel is null. Do not close it");
		} else {
			try {
				channel.close();
			} catch (Exception e) {
				LOGGER.warn("Fail to close channel: {}", e.getMessage(), e);
			}
		}
	}

	/**
	 * Gets the channel for specified connectionType.
	 * 
	 * @param connectionType
	 * @param rmqConnection
	 * @return channel instance
	 * @throws Exception
	 */
	private synchronized Channel borrowChannel(ConnectionType connectionType, Connection rmqConnection)
			throws Exception {
		if (!RMQ_CHANNEL_MAP.containsKey(connectionType)) {
			Channel channel = getOrCreateChannel(connectionType, rmqConnection);
			LOGGER.info(String.format(MessageConstants.INFO_CHANNEL_CREATION_SUCCESS, connectionType));
			return channel;
		}
		Set<Channel> channels = RMQ_CHANNEL_MAP.get(connectionType);
		if (channels != null && channels.isEmpty()) {
			Channel channel = getOrCreateChannel(connectionType, rmqConnection);
			LOGGER.info(String.format(MessageConstants.INFO_CHANNEL_CREATION_SUCCESS, connectionType));
			return channel;
		}
		Iterator<Channel> itr = channels.iterator();
		while (itr.hasNext()) {
			Channel channel = itr.next();
			if (channel != null && channel.isOpen()) {
				itr.remove();
				LOGGER.info(String.format(MessageConstants.INFO_CHANNEL_BORROW_SUCCESS, connectionType));
				return channel;
			} else {
				itr.remove();
			}
		}
		Channel channel = getOrCreateChannel(connectionType, rmqConnection);
		LOGGER.info(String.format(MessageConstants.INFO_CHANNEL_RESET_SUCCESS, connectionType));
		return channel;
	}

	/**
	 * Returns the channel to connection pool for specified connectionType.
	 * 
	 * @param connectionType
	 * @param channel
	 * @throws Exception
	 */
	public synchronized void returnChannel(ConnectionType connectionType, Channel channel) throws Exception {
		if (channel == null || !channel.isOpen()) {
			channel = null; // channel is reset.
		}
		Set<Channel> channels = RMQ_CHANNEL_MAP.get(connectionType);
		if (channels == null) {
			channels = new HashSet<Channel>();
			RMQ_CHANNEL_MAP.put(connectionType, channels);
		}
		channels.add(channel);
		LOGGER.info(String.format(MessageConstants.INFO_CHANNEL_RETURN_SUCCESS, connectionType));
	}

}

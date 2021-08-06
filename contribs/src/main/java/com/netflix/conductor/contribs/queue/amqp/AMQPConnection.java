package com.netflix.conductor.contribs.queue.amqp;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.Address;

public class AMQPConnection {
	
	private static Logger logger = LoggerFactory.getLogger(AMQPConnection.class);	
	private volatile Connection publisherConnection = null;
	private volatile Connection subscriberConnection = null;
	private volatile Channel publisherChannel = null;
	private volatile Channel subscriberChannel = null;
	private ConnectionFactory factory = null;
	private Address[] addresses = null;
	private static AMQPConnection amqpConnection = null;
	private final static String PUBLISHER = "Publisher";
	private final static String SUBSCRIBER = "Subscriber";
	
	private AMQPConnection() {
		
	}
	
	private AMQPConnection(final ConnectionFactory factory,final Address[] address)
	{		
		this.factory = factory;
		this.addresses = address;		
	}
	
	public static synchronized AMQPConnection getInstance(final ConnectionFactory factory, final Address[] address)
	{
		if (amqpConnection == null)
		{
			amqpConnection = new AMQPConnection(factory,address);
		}
		
		return amqpConnection;
	}
	
	public Address[] getAddresses() {
		return   addresses;
	}
	
	private Connection createConnection(String connectionPrefix) {
			
			logger.info("Creating connection for {} instance {}" , connectionPrefix, System.identityHashCode(this));
			try {
				Connection connection = factory.newConnection(addresses, System.getenv("HOSTNAME") + "-" + connectionPrefix+ "-" + System.identityHashCode(this));			
				if (connection == null || !connection.isOpen()) {
					throw new RuntimeException("Failed to open connection");
				}
				
				connection.addShutdownListener(new ShutdownListener() {
					
					@Override
					public void shutdownCompleted(ShutdownSignalException cause) {
						logger.error("Received a shutdown exception for the connection {}. reason {} cause{}",connection.getClientProvidedName(),cause.getMessage(),cause);
						
					}
				});
				
				connection.addBlockedListener(new BlockedListener() {				
					@Override
					public void handleUnblocked() throws IOException {
						logger.info("Connection {} is unblocked",connection.getClientProvidedName());					
					}
					
					@Override
					public void handleBlocked(String reason) throws IOException {
						logger.error("Connection {} is blocked. reason: {}",connection.getClientProvidedName(),reason);					
					}
				});
				
				return connection;			
			} catch (final IOException e) {			
				final String error = "IO error while connecting to "
						+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(","));
				logger.error(error, e);
				throw new RuntimeException(error, e);
			} catch (final TimeoutException e) {			
				final String error = "Timeout while connecting to "
						+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(","));
				logger.error(error, e);
				throw new RuntimeException(error, e);
			}
		}
	
	
	public  Channel getOrCreateSubscriberChannel() {		
		// Return the existing channel if it's still opened
		if (subscriberChannel != null ) {
			return subscriberChannel;
		}
		// Channel creation is required
		try {			
			if (subscriberConnection == null) {
				synchronized (this) {					
					if(subscriberConnection == null) {
						subscriberConnection = createConnection(SUBSCRIBER);
					}
					logger.debug("Creating a channel for subscriber");
					subscriberChannel = subscriberConnection.createChannel();
					subscriberChannel.addShutdownListener(cause -> {				
						logger.error("subscription Channel has been shutdown: {}", cause.getMessage(), cause);			
						
					});
				}				
			}		
			
		} catch (final IOException e) {
			throw new RuntimeException("Cannot open subscription channel on "
					+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(",")), e);
		}
		
		if (subscriberChannel == null || !subscriberChannel.isOpen()) {
			throw new RuntimeException("Fail to open  subscription channel");
		}
		return subscriberChannel;
	}
	
	public Channel getOrCreatePublisherChannel() {		
		// Return the existing channel 
		if (publisherChannel != null ) {
			return publisherChannel;
		}
		// Channel creation is required
		try {	
			if (publisherConnection == null) {				
				synchronized (this) {
					if (publisherConnection == null) {
						publisherConnection = createConnection(PUBLISHER);
					}
					
					logger.debug("Creating a channel for publisher");
					publisherChannel = publisherConnection.createChannel();
					publisherChannel.addShutdownListener(cause -> {				
						logger.error("Publish Channel has been shutdown: {}", cause.getMessage(), cause);
					});
				}				
			}
						
		} catch (final IOException e) {
			throw new RuntimeException("Cannot open channel on "
					+ Arrays.stream(addresses).map(address -> address.toString()).collect(Collectors.joining(",")), e);
		}
		
		if (publisherChannel == null || !publisherChannel.isOpen()) {
			throw new RuntimeException("Fail to open publish channel");
		}
		return publisherChannel;
	}
	
	public void close() {
		logger.info("Closing all connections and channels");
		try {
			closeChannel(publisherChannel);		
			closeConnection(publisherConnection);
			closeChannel(subscriberChannel);
			closeConnection(subscriberConnection);
		}finally {
			publisherChannel = null;
			publisherConnection = null;
			subscriberChannel = null;
			subscriberConnection = null;
		}
	}
	
	
	private void closeConnection(Connection connection) {
		if (connection == null) {
			logger.warn("Connection is null. Do not close it");
		} else {
			try {
				connection.close();
			} catch (final Exception e) {
				logger.warn("Fail to close connection: {}", e.getMessage(), e);
			}
		}
	}
	

	private void closeChannel(Channel channel) {
		if (channel == null) {
			logger.warn("Channel is null. Do not close it");
		} else {		
			try {				
				channel.close();
			} catch (final Exception e) {
				logger.warn("Fail to close channel: {}", e.getMessage(), e);
			}
		}				
	}
	
	

}

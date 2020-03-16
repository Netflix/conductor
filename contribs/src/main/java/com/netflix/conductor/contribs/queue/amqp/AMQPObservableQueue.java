package com.netflix.conductor.contribs.queue.amqp;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.contribs.http.HttpTask;
import com.netflix.conductor.contribs.queue.AbstractObservableQueue;
import com.netflix.conductor.core.events.queue.Message;

import rx.Observable;

public class AMQPObservableQueue extends AbstractObservableQueue {
	 private static Logger logger = LoggerFactory.getLogger(AMQPObservableQueue.class);

	    private final AMQPSettings settings;

	    private final int batchSize;

	    private boolean useExchange;

	    private boolean isConnOpened = false, isChanOpened = false;

	    private ConnectionFactory factory;
	    private Connection connection;
	    private Channel channel;
	    private Address[] addresses;

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
	        this.factory = factory;
	        this.addresses = addresses;
	        this.useExchange = useExchange;
	        this.settings = settings;
	        this.batchSize = batchSize;
	        this.pollTimeInMS = pollTimeInMS;
	    }

		@Override
		public Observable<Message> observe() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getName() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getURI() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<String> ack(List<Message> messages) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void publish(List<Message> messages) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void setUnackTimeout(Message message, long unackTimeout) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public long size() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		protected List<Message> receiveMessages() {
			// TODO Auto-generated method stub
			return null;
		}
}

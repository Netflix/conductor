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
/**
 *
 */
package com.netflix.conductor.contribs.queue.nats;

import com.netflix.conductor.core.events.EventQueues;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Oleksiy Lysak
 */
public class NATSObservableQueue extends NATSAbstractQueue {
	private static Logger logger = LoggerFactory.getLogger(NATSObservableQueue.class);
	private final AtomicReference<Connection> conn = new AtomicReference<>();
	private final AtomicReference<Dispatcher> disp = new AtomicReference<>();
	private final Properties props;

	public NATSObservableQueue(Properties props, String queueURI, int[] delays) {
		super(queueURI, EventQueues.QueueType.nats, delays);
		this.props = props;
		open();
	}

	@Override
	public boolean isConnected() {
		return (conn.get() != null
				&& conn.get().getStatus() == Connection.Status.CONNECTED);
	}

	@Override
	public void connect() {
		try {
			Options options = new Options.Builder(props).build();
			Connection temp = Nats.connect(options);
			logger.debug("Successfully connected for " + queueURI);

			conn.set(temp);
		} catch (Exception e) {
			logger.error("Unable to establish nats connection for " + queueURI, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void subscribe() {
		if (isSubscribed()) {
			return;
		}

		try {
			ensureConnected();

			Dispatcher dispatcher = conn.get().createDispatcher(msg -> onMessage(msg.getSubject(), msg.getData()));

			// Create subject/queue subscription if the queue has been provided
			if (StringUtils.isNotEmpty(queue)) {
				logger.debug("No subscription. Creating a queue subscription. subject={}, queue={}", subject, queue);
				dispatcher.subscribe(subject, queue);
			} else {
				logger.debug("No subscription. Creating a pub/sub subscription. subject={}", subject);
				dispatcher.subscribe(subject);
			}

			disp.set(dispatcher);
		} catch (Exception ex) {
			logger.error("Subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
		}
	}

	@Override
	public boolean isSubscribed() {
		return disp.get() != null;
	}

	@Override
	public void publish(String subject, byte[] data) throws Exception {
		ensureConnected();
		conn.get().publish(subject, data);
	}

	@Override
	public void closeConn() {
		if (conn.get() != null) {
			close(conn.get());
			conn.set(null);
		}
		if (disp.get() != null) {
			disp.set(null);
		}
	}
}

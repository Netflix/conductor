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
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Oleksiy Lysak
 */
public class NATSStreamObservableQueue extends NATSAbstractQueue {
	private static Logger logger = LoggerFactory.getLogger(NATSStreamObservableQueue.class);
	private final AtomicReference<StreamingConnection> conn = new AtomicReference<>();
	private final AtomicReference<Subscription> subs = new AtomicReference<>();
	private final StreamingConnectionFactory fact;
	private final String durableName;

	public NATSStreamObservableQueue(String clusterId, String natsUrl, String durableName, String queueURI, int[] delays) {
		super(queueURI, EventQueues.QueueType.nats_stream, delays);
		this.fact = new StreamingConnectionFactory();
		this.fact.setClusterId(clusterId);
		this.fact.setClientId(UUID.randomUUID().toString());
		this.fact.setNatsUrl(natsUrl);
		this.durableName = durableName;
		open();
	}

	@Override
	public boolean isConnected() {
		return (conn.get() != null
				&& conn.get().getNatsConnection() != null
				&& conn.get().getNatsConnection().getStatus() == Connection.Status.CONNECTED);
	}

	@Override
	public void connect() {
		try {
			StreamingConnection temp = fact.createConnection();
			logger.debug("Successfully connected for " + queueURI);

			conn.set(temp);
		} catch (Exception e) {
			logger.error("Unable to establish nats streaming connection for " + queueURI, e);
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

			SubscriptionOptions options = new SubscriptionOptions.Builder()
					.durableName(durableName)
					.build();

			StreamingConnection tmpConn = conn.get();
			Subscription tmpSubs;

			// Create subject/queue subscription if the queue has been provided
			if (StringUtils.isNotEmpty(queue)) {
				logger.debug("No subscription. Creating a queue subscription. subject={}, queue={}", subject, queue);
				tmpSubs = tmpConn.subscribe(subject, queue, msg -> onMessage(msg.getSubject(), msg.getData()), options);
			} else {
				logger.debug("No subscription. Creating a pub/sub subscription. subject={}", subject);
				tmpSubs = tmpConn.subscribe(subject, msg -> onMessage(msg.getSubject(), msg.getData()), options);
			}

			subs.set(tmpSubs);
		} catch (Exception ex) {
			logger.error("Subscription failed with " + ex.getMessage() + " for queueURI " + queueURI, ex);
		}
	}

	@Override
	public boolean isSubscribed() {
		return subs.get() != null;
	}

	@Override
	public void publish(String subject, byte[] data) throws Exception {
		ensureConnected();
		conn.get().publish(subject, data);
	}

	@Override
	public void closeConn() {
		if (subs.get() != null) {
			close(subs.get());
			subs.set(null);
		}
		if (conn.get() != null) {
			close(conn.get());
			conn.set(null);
		}
	}
}

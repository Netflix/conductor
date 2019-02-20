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
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import io.nats.client.NUID;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Oleksiy Lysak
 */
public abstract class NATSAbstractQueue implements ObservableQueue {
	private static Logger logger = LoggerFactory.getLogger(NATSAbstractQueue.class);
	protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
	private AtomicBoolean listened = new AtomicBoolean();
	private AtomicBoolean opened = new AtomicBoolean();
	private EventQueues.QueueType queueType;
	private ScheduledExecutorService execs;
	private int[] publishRetryIn;
	final String queueURI;
	final String subject;
	final String queue;

	NATSAbstractQueue(String queueURI, EventQueues.QueueType queueType, int[] publishRetryIn) {
		this.queueURI = queueURI;
		this.queueType = queueType;
		this.publishRetryIn = publishRetryIn;

		// If queue specified (e.g. subject:queue) - split to subject & queue
		if (queueURI.contains(":")) {
			this.subject = queueURI.substring(0, queueURI.indexOf(':'));
			this.queue = queueURI.substring(queueURI.indexOf(':') + 1);
		} else {
			this.subject = queueURI;
			this.queue = null;
		}
		logger.debug(String.format("Initialized with queueURI=%s, subject=%s, queue=%s", queueURI, subject, queue));
	}

	void onMessage(String subject, byte[] data) {
		String payload = new String(data);
		logger.info(String.format("Received message for %s: %s", subject, payload));

		Message dstMsg = new Message();
		dstMsg.setId(NUID.nextGlobal());
		dstMsg.setPayload(payload);

		messages.add(dstMsg);
		Monitors.recordEventQueueMessagesReceived(queueType.name(), queueURI);
	}

	@Override
	public Observable<Message> observe() {
		logger.debug("Observe invoked for queueURI " + queueURI);
		listened.set(true);

		subscribe();

		Observable.OnSubscribe<Message> onSubscribe = subscriber -> {
			Observable<Long> interval = Observable.interval(100, TimeUnit.MILLISECONDS);
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

					logger.debug(String.format("Batch from %s to conductor is %s", subject, buffer.toString()));
				}

				return Observable.from(available);
			}).subscribe(subscriber::onNext, subscriber::onError);
		};
		return Observable.create(onSubscribe);
	}

	@Override
	public String getType() {
		return queueType.name();
	}

	@Override
	public String getName() {
		return queueURI;
	}

	@Override
	public String getURI() {
		return queueURI;
	}

	@Override
	public List<String> ack(List<Message> messages) {
		return Collections.emptyList();
	}

	@Override
	public void setUnackTimeout(Message message, long unackTimeout) {
	}

	@Override
	public long size() {
		return messages.size();
	}

	@Override
	public void publish(List<Message> messages) {
		messages.forEach(message -> {
			String payload = message.getPayload();
			try {
				logger.debug(String.format("Trying to publish to %s: %s", subject, payload));
				publishWait(subject, payload);
				logger.info(String.format("Published to %s: %s", subject, payload));
			} catch (Exception eo) {
				logger.error(String.format("Failed to publish to %s: %s", subject, payload), eo);
				closeConn();
				if (ArrayUtils.isEmpty(publishRetryIn)) {
					throw new RuntimeException(eo);
				}
				for (int i = 0; i < publishRetryIn.length; i++) {
					try {
						int delay = publishRetryIn[i];
						logger.error(String.format("Publish retry in %s seconds for %s", delay, subject));

						Thread.sleep(delay * 1000L);

						logger.debug(String.format("Trying to publish to %s: %s", subject, payload));
						publishWait(subject, payload);
						logger.info(String.format("Published to %s: %s", subject, payload));

						break;
					} catch (Exception ex) {
						logger.error(String.format("Failed to publish to %s: %s", subject, payload), ex);
                        closeConn();
						// Latest attempt
						if (i == (publishRetryIn.length - 1)) {
							logger.error(String.format("Publish gave up for %s: %s", subject, payload));
							throw new RuntimeException(ex.getMessage(), ex);
						}
					}
				}
			}
		});
	}

	@Override
	public void close() {
		logger.debug("Closing connection for " + queueURI);
		if (execs != null) {
			execs.shutdownNow();
			execs = null;
		}
		closeConn();
		opened.set(false);
	}

	public void open() {
		// do nothing if not closed
		if (opened.get()) {
			return;
		}

		try {
			connect();

			// Re-initiated subscription if existed
			if (listened.get()) {
				subscribe();
			}
		} catch (Exception ignore) {
		}

		execs = Executors.newScheduledThreadPool(1);
		execs.scheduleAtFixedRate(this::monitor, 0, 500, TimeUnit.MILLISECONDS);
		opened.set(true);
	}

	private void monitor() {
		try {
			boolean observed = listened.get();
			boolean connected = isConnected();
			boolean subscribed = isSubscribed();

			// All conditions are met
			if (connected && observed && subscribed) {
				return;
			} else if (connected && !observed && !subscribed) {
				return;
			}

			logger.error("Monitor invoked for " + queueURI);
			closeConn();

			// Connect
			connect();

			// Re-initiated subscription if existed
			if (observed) {
				subscribe();
			}
		} catch (Exception ex) {
			logger.error("Monitor failed with " + ex.getMessage() + " for " + queueURI, ex);
		}
	}

	private void publishWait(String subject, String payload) throws Exception {
		LinkedBlockingDeque<Object> queue = new LinkedBlockingDeque<>();

		Runnable task = () -> {
			try {
				publish(subject, payload.getBytes());
				queue.add(Boolean.TRUE);
			} catch (Exception ex) {
				queue.add(ex);
			}
		};

		Thread thread = new Thread(task);
		thread.start();

		// Publish must be really fast and no need to wait longer than even 3 seconds
		Object result = queue.poll(3, TimeUnit.SECONDS);
		if (result == null) {
			thread.interrupt();
			throw new RuntimeException("Publish timed out");
		} else if (result instanceof Exception) {
			Exception ex = (Exception)result;
			throw new RuntimeException(ex.getMessage(), ex);
		}
	}

	public boolean isClosed() {
		return !opened.get();
	}

	void ensureConnected() {
		if (!isConnected()) {
			throw new RuntimeException("No nats connection");
		}
	}

	void close(AutoCloseable closeable) {
		try {
			closeable.close();
		} catch (Exception ignore) {
		}
	}

	abstract void connect();

	abstract boolean isConnected();

	abstract boolean isSubscribed();

	abstract void publish(String subject, byte[] data) throws Exception;

	abstract void subscribe();

	abstract void closeConn();
}

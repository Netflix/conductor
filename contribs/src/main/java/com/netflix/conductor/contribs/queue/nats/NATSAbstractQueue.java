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
import io.nats.client.NUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Oleksiy Lysak
 *
 */
public abstract class NATSAbstractQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(NATSAbstractQueue.class);
    protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    protected final Lock mu = new ReentrantLock();

    private EventQueues.QueueType queueType;
    String queueURI;
    String subject;
    String queue;

    // Indicates that observe was called (Event Handler) and we must to re-initiate subscription upon reconnection
    boolean observable;

    NATSAbstractQueue(String queueURI, EventQueues.QueueType queueType) {
        this.queueURI = queueURI;
        this.queueType = queueType;

        // If queue specified (e.g. subject:queue) - split to subject & queue
        if (queueURI.contains(":")) {
            this.subject = queueURI.substring(0, queueURI.indexOf(':'));
            this.queue = queueURI.substring(queueURI.indexOf(':') + 1);
        } else {
            this.subject = queueURI;
            this.queue = null;
        }
        logger.info(String.format("Initialized with queueURI=%s, subject=%s, queue=%s", queueURI, subject, queue));
    }

    void onMessage(String subject, byte[] data) {
        String payload = new String(data);
        logger.info(String.format("Received message for %s: %s", subject, payload));

        Message dstMsg = new Message();
        dstMsg.setId(NUID.nextGlobal());
        dstMsg.setPayload(payload);

        messages.add(dstMsg);
    }

    @Override
    public Observable<Message> observe() {
        logger.info("Observe invoked for queueURI " + queueURI);
        observable = true;

        mu.lock();
        try {
            subscribe();
        } finally {
            mu.unlock();
        }

        Observable.OnSubscribe<Message> action = subscriber -> {
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

                    logger.info(String.format("Batch from %s to conductor are %s ", subject, buffer.toString()));
                }

                return Observable.from(available);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
        return Observable.create(action);
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
            try {
                String payload = message.getPayload();
                publish(subject, payload.getBytes());
                logger.info(String.format("Published message to %s: %s", subject, payload));
            } catch (Exception ex) {
                logger.error("Failed to publish message " + message + " to " + subject, ex);
                throw new RuntimeException(ex);
            }
        });
    }

    abstract void publish(String subject, byte[] data) throws Exception;

    abstract void subscribe();
}

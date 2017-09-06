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

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import io.nats.client.NUID;
import io.nats.stan.Connection;
import io.nats.stan.SubscriptionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 *
 */
public class NATSObservableQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(NATSObservableQueue.class);
    private final SubscriptionOptions.Builder builder = new SubscriptionOptions.Builder();
    private LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private static final String TYPE = "nats";
    private Connection connection;
    private String subject;

    public NATSObservableQueue(Connection connection, String subject, String qgroup, String durableName) {
        this.connection = connection;
        this.subject = subject;
        try {
            SubscriptionOptions.Builder builder = new SubscriptionOptions.Builder().setDurableName(durableName);
            connection.subscribe(subject, qgroup, natMsg -> {
                Message dstMsg = new Message();
                dstMsg.setId(NUID.nextGlobal());
                dstMsg.setPayload(new String(natMsg.getData()));

                logger.info("Received message from NATs\n" + new String(natMsg.getData()));
                messages.add(dstMsg);
            }, builder.build());
        } catch (Exception e) {
            logger.error("Unable to start subscription for " + subject + " @ " + qgroup, e);
        }
    }

    @Override
    public Observable<Message> observe() {
        return Observable.create(getOnSubscribe());
    }

    private Observable.OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(100, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x) -> {
                List<Message> available = new LinkedList<>();
                messages.drainTo(available);
                return Observable.from(available);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return subject;
    }

    @Override
    public String getURI() {
        return subject;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        return Collections.emptyList();
    }

    @Override
    public void publish(List<Message> messages) {
        messages.forEach(message -> {
            try {
                connection.publish(subject, message.getPayload().getBytes());
            } catch (IOException e) {
                logger.error("Failed to publish message {}", message);
            }
        });

    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return 0;
    }
}

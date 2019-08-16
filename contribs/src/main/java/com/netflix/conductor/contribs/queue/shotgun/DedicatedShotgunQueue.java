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
package com.netflix.conductor.contribs.queue.shotgun;

import com.bydeluxe.onemq.OneMQ;
import com.bydeluxe.onemq.OneMQClient;
import com.bydeluxe.onemq.Subscription;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.events.queue.OnMessageHandler;
import d3sw.shotgun.shotgunpb.ShotgunOuterClass;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Oleksiy Lysak
 */
public class DedicatedShotgunQueue extends SharedShotgunQueue implements ObservableQueue {

    public DedicatedShotgunQueue(String dns, String service, String queueURI, Duration[] publishRetryIn,
                                 boolean manualAck, int prefetchSize, OnMessageHandler handler) {
        super(null, service, queueURI, publishRetryIn, manualAck, prefetchSize, handler);
        try {
            conn = new OneMQ();
            conn.connect(dns, null, null);
        } catch (Exception ex) {
            logger.debug("OneMQ client connect failed {}", ex.getMessage(), ex);
        }
    }

    @Override
    public void close() {
        if (subs != null) {
            try {
                conn.unsubscribe(subs);
            } catch (Exception ignore) {
            }
            subs = null;
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignore) {
            }
            conn = null;
        }
        logger.debug("Closed for " + queueURI);
    }
}

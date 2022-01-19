/*
 * Copyright 2022 Netflix, Inc.
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
package com.netflix.conductor.contribs.queue.amqp.config;

import com.netflix.conductor.contribs.queue.amqp.util.RetryType;

public class AMQPRetryPattern {

    private int limit = 50;
    private int duration = 1000;
    private RetryType type = RetryType.REGULARINTERVALS;

    public AMQPRetryPattern(int limit, int duration, RetryType type) {
        this.limit = limit;
        this.duration = duration;
        this.type = type;
    }

    /**
     * Based on the retry configuration, this gets executed if the retry index is within the allowed
     * limits
     *
     * @throws Exception
     */
    public void continueOrPropogate(Exception ex, int retryIndex) throws Exception {
        if (retryIndex > limit) {
            throw ex;
        }
        // Regular Intervals is the default
        long waitDuration = duration;
        if (type == RetryType.INCREMENTALINTERVALS) {
            waitDuration = duration * retryIndex;
        } else if (type == RetryType.EXPONENTIALBACKOFF) {
            waitDuration = (long) Math.pow(2, retryIndex) * duration;
        }
        try {
            Thread.sleep(waitDuration);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}

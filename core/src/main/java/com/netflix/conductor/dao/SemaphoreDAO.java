/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.dao;

import com.netflix.conductor.common.metadata.tasks.Task;

/**
 * An abstraction for a non-blocking semaphore object
 */
public interface SemaphoreDAO {

    /**
     * Try to acquire the semaphore
     * @param semaphoreName: The name of the semaphore
     * @param identifier: A unique identifier that identifies the holder
     * @param limit: This semaphore's maximum number of holders limit. If changes between calls for the same semaphore name, behavior is undefined.
     * @param timeoutMillis: A timeout for all the holders of the semaphore If changes between calls for the same semaphore name, behavior is undefined.
     * @return true: If the semaphore was successfully acquired
     * 		false: If the semaphore was successfully not acquired (e.g. Reached limit)
     */
    boolean tryAcquire(String semaphoreName, String identifier, int limit, double timeoutMillis);

    /**
     * Release the semaphore
     * @param semaphoreName: The name of the semaphore
     * @param identifier: A unique identifier that identifies the holder
     * @return true: If the semaphore was successfully released
     * 		false: If the semaphore was successfully not released (e.g. Never acquired in the first place)
     */
    boolean release(String semaphoreName, String identifier);
}

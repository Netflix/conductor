/*
 * Copyright (c) 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.conductor.core.utils;

import java.util.concurrent.TimeUnit;

/**
 * Interface implemented by a distributed lock client.
 *
 * A typical usage:
 *   if (acquireLock(workflowId, 5, TimeUnit.MILLISECONDS)) {
 *      [load and execute workflow....]
 *      ExecutionDAO.updateWorkflow(workflow);  //use optimistic locking
 *   } finally {
 *     releaseLock(workflowId)
 *   }
 *
 */

public interface Lock {

    /**
     * acquires a re-entrant lock on lockId, blocks indefinitely on lockId until it succeeds
     * @param lockId resource to lock on
     */
    void acquireLock(String lockId);

    /**
     * acquires a re-entrant lock on lockId, blocks for timeToTry duration before giving up
     * @param lockId resource to lock on
     * @param timeToTry blocks up to timeToTry duration in attempt to acquire the lock
     * @param unit time unit
     * @return
     */
    boolean acquireLock(String lockId, long timeToTry, TimeUnit unit);

    /**
     * acquires a re-entrant lock on lockId with provided leaseTime duration. Blocks for timeToTry duration before giving up
     * @param lockId resource to lock on
     * @param timeToTry blocks up to timeToTry duration in attempt to acquire the lock
     * @param leaseTime Lock lease expiration duration.
     * @param unit time unit
     * @return
     */
    boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit);

    /**
     * Release a previously acquired lock
     * @param lockId resource to lock on
     */
    void releaseLock(String lockId);

    /**
     * Explicitly cleanup lock resources, if releasing it wouldn't do so.
     * @param lockId resource to lock on
     */
    void deleteLock(String lockId);
}

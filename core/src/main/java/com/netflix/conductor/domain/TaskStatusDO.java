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
package com.netflix.conductor.domain;

import com.netflix.conductor.common.metadata.tasks.Task.Status;

public enum TaskStatusDO {
    IN_PROGRESS(false, true, true),
    CANCELED(true, false, false),
    FAILED(true, false, true),
    FAILED_WITH_TERMINAL_ERROR(
            true, false,
            false), // No retries even if retries are configured, the task and the related
    // workflow should be terminated
    COMPLETED(true, true, true),
    COMPLETED_WITH_ERRORS(true, true, true),
    SCHEDULED(false, true, true),
    TIMED_OUT(true, false, true),
    SKIPPED(true, true, false);

    private final boolean terminal;

    private final boolean successful;

    private final boolean retriable;

    TaskStatusDO(boolean terminal, boolean successful, boolean retriable) {
        this.terminal = terminal;
        this.successful = successful;
        this.retriable = retriable;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public static Status getTaskStatusDTO(TaskStatusDO taskStatusDO) {
        return Status.valueOf(taskStatusDO.name());
    }
}

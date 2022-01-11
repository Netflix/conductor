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

import com.netflix.conductor.common.run.Workflow.WorkflowStatus;

public enum WorkflowStatusDO {
    RUNNING(false, false),
    COMPLETED(true, true),
    FAILED(true, false),
    TIMED_OUT(true, false),
    TERMINATED(true, false),
    PAUSED(false, true);

    private final boolean terminal;
    private final boolean successful;

    WorkflowStatusDO(boolean terminal, boolean successful) {
        this.terminal = terminal;
        this.successful = successful;
    }

    public boolean isTerminal() {
        return terminal;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public static WorkflowStatus getWorkflowStatusDTO(WorkflowStatusDO workflowStatusDO) {
        return WorkflowStatus.valueOf(workflowStatusDO.name());
    }
}

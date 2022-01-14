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
package com.netflix.conductor.core.listener;

import com.netflix.conductor.domain.WorkflowDO;

/** Listener for the completed and terminated workflows */
public interface WorkflowStatusListener {

    default void onWorkflowCompletedIfEnabled(WorkflowDO workflow) {
        if (workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            onWorkflowCompleted(workflow);
        }
    }

    default void onWorkflowTerminatedIfEnabled(WorkflowDO workflow) {
        if (workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            onWorkflowTerminated(workflow);
        }
    }

    default void onWorkflowFinalizedIfEnabled(WorkflowDO workflow) {
        if (workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            onWorkflowFinalized(workflow);
        }
    }

    void onWorkflowCompleted(WorkflowDO workflow);

    void onWorkflowTerminated(WorkflowDO workflow);

    default void onWorkflowFinalized(WorkflowDO workflow) {}
}

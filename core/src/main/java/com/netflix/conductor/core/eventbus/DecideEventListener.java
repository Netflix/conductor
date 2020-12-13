package com.netflix.conductor.core.eventbus;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import javax.inject.Inject;

public class DecideEventListener {

    @Inject
    private WorkflowExecutor workflowExecutor;

    @Subscribe
    @AllowConcurrentEvents
    public void onDecideEvent(DecideEvent decideEvent) {
        workflowExecutor.decide(decideEvent.getWorkflowId());
    }
}

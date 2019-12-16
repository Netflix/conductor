package com.netflix.conductor.core.execution;


import com.netflix.conductor.common.run.Workflow;

public interface WorkflowStatusPublisher {
    void onCompleted(Workflow workflow);
}

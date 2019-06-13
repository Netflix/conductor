package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;

import java.util.List;

public interface WorkflowObfuscationQueuePublisher {

    void publish(Workflow workflow);

    void publishAll(List<String> workflowIds, WorkflowDef workflowDef);

}

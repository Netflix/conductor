package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WorkflowObfuscationQueuePublisherStub implements WorkflowObfuscationQueuePublisher {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowObfuscationQueuePublisherStub.class);

    @Override
    public void publish(Workflow workflow) {
        LOG.debug("workflow: {} published for obfuscation process", workflow.getWorkflowId());
    }

    @Override
    public void publishAll(List<String> workflowIds, WorkflowDef workflowDef) {
        LOG.debug("workflows: {} published for obfuscation process", workflowIds);
    }


}

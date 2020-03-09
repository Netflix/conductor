package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.archival.WorkflowArchival;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ArchiveWorkflowViaReminderStatusListener implements WorkflowStatusListener {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveWorkflowViaReminderStatusListener.class);

    @Inject
    private WorkflowArchival workflowArchival;

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        LOG.debug("Workflow {} is completed", workflow.getWorkflowId());
        workflowArchival.archiveWorkflow(workflow.getWorkflowId());
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        LOG.debug("Workflow {} is terminated", workflow.getWorkflowId());
        workflowArchival.archiveWorkflow(workflow.getWorkflowId());
    }

}

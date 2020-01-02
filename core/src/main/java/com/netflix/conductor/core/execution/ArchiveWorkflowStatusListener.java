package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class ArchiveWorkflowStatusListener implements WorkflowStatusListener {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveWorkflowStatusListener.class);
    private final ExecutionDAOFacade executionDAOFacade;
    private final Configuration configuration;

    @Inject
    public ArchiveWorkflowStatusListener(ExecutionDAOFacade executionDAOFacade, Configuration configuration) {
        this.executionDAOFacade = executionDAOFacade;
        this.configuration = configuration;
    }

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        LOG.debug("Workflow {} is completed", workflow.getWorkflowId());
        if (configuration.isForceWorkflowDeletionEnabled() || workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            removeWorkflow(workflow.getWorkflowId(), true);
        }
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        LOG.debug("Workflow {} is terminated", workflow.getWorkflowId());
        if (configuration.isForceWorkflowDeletionEnabled() || workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            removeWorkflow(workflow.getWorkflowId(), true);
        }
    }

    private void removeWorkflow(String workflowId, boolean shouldArchive) {
        executionDAOFacade.removeWorkflow(workflowId, shouldArchive);
    }

}

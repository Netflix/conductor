package com.netflix.conductor.core.execution;

import com.google.inject.AbstractModule;
import com.netflix.conductor.core.execution.ArchiveWorkflowStatusListener;

/**
 * Default implementation for the workflow status listener
 *
 */
public class WorkflowExecutorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(WorkflowStatusListener.class).to(ArchiveWorkflowListener.class);
    }
}

package com.netflix.conductor.core.execution;

import com.google.inject.AbstractModule;
import com.netflix.conductor.service.*;


/**
 * Default implementation for the workflow status listener
 */
public class WorkflowExecutorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(WorkflowStatusListener.class).to(ArchiveWorkflowViaReminderStatusListener.class);
        //service layer
        bind(AdminService.class).to(AdminServiceImpl.class);
        bind(WorkflowService.class).to(WorkflowServiceImpl.class);
        bind(WorkflowBulkService.class).to(WorkflowBulkServiceImpl.class);
        bind(TaskService.class).to(TaskServiceImpl.class);
        bind(EventService.class).to(EventServiceImpl.class);
        bind(MetadataService.class).to(MetadataServiceImpl.class);
    }
}

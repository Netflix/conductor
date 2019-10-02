package com.netflix.conductor.grpc.server;

import com.google.inject.AbstractModule;

import com.google.inject.TypeLiteral;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.grpc.EventServiceGrpc;
import com.netflix.conductor.grpc.MetadataServiceGrpc;
import com.netflix.conductor.grpc.TaskServiceGrpc;
import com.netflix.conductor.grpc.WorkflowServiceGrpc;
import com.netflix.conductor.grpc.server.service.EventServiceImpl;
import com.netflix.conductor.grpc.server.service.HealthServiceImpl;
import com.netflix.conductor.grpc.server.service.MetadataServiceImpl;
import com.netflix.conductor.grpc.server.service.TaskServiceImpl;
import com.netflix.conductor.grpc.server.service.WorkflowServiceImpl;

import com.netflix.conductor.service.MetadataService;
import com.netflix.conductor.service.TaskService;
import com.netflix.conductor.service.WorkflowService;
import io.grpc.health.v1.HealthGrpc;

public class GRPCModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(HealthGrpc.HealthImplBase.class).to(HealthServiceImpl.class);

        bind(EventServiceGrpc.EventServiceImplBase.class).to(EventServiceImpl.class);
        bind(MetadataServiceGrpc.MetadataServiceImplBase.class).to(MetadataServiceImpl.class);
        bind(TaskServiceGrpc.TaskServiceImplBase.class).to(TaskServiceImpl.class);
        bind(WorkflowServiceGrpc.WorkflowServiceImplBase.class).to(WorkflowServiceImpl.class);

        bind(new TypeLiteral<MetadataService<TaskDef, WorkflowDef, EventHandler>>() {}).to(
            com.netflix.conductor.service.MetadataServiceImpl.class);
        bind(new TypeLiteral<WorkflowService<Workflow, WorkflowDef, WorkflowSummary>>() {}).to(
            com.netflix.conductor.service.WorkflowServiceImpl.class);
        bind(new TypeLiteral<TaskService<Task, PollData, TaskSummary>>() {}).to(
            com.netflix.conductor.service.TaskServiceImpl.class);

        bind(GRPCServerConfiguration.class).to(GRPCServerSystemConfiguration.class);
        bind(GRPCServerProvider.class);
    }
}

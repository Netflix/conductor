/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.conductor.service;

import com.netflix.conductor.annotations.Audit;
import com.netflix.conductor.annotations.Service;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.constraints.OwnerEmailMandatoryConstraint;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.validations.ValidationContext;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

@Audit
@Singleton
@Trace
public class MetadataServiceImpl implements MetadataService {

    private final MetadataDAO metadataDAO;
    private final EventHandlerDAO eventHandlerDAO;
    private final EventQueues eventQueues;

    @Inject
    public MetadataServiceImpl(MetadataDAO metadataDAO, EventHandlerDAO eventHandlerDAO, EventQueues eventQueues,
                               Configuration configuration) {
        this.metadataDAO = metadataDAO;
        this.eventHandlerDAO = eventHandlerDAO;
        this.eventQueues = eventQueues;

        ValidationContext.initialize(metadataDAO);
        OwnerEmailMandatoryConstraint.WorkflowTaskValidValidator.setOwnerEmailMandatory(configuration.isOwnerEmailMandatory());
    }

    /**
     * @param taskDefinitions Task Definitions to register
     */
    @Service
    public void registerTaskDef(List<TaskDef> taskDefinitions) {
        for (TaskDef taskDefinition : taskDefinitions) {
            taskDefinition.setCreatedBy(WorkflowContext.get().getClientApp());
            taskDefinition.setCreateTime(System.currentTimeMillis());
            taskDefinition.setUpdatedBy(null);
            taskDefinition.setUpdateTime(null);

            metadataDAO.createTaskDef(taskDefinition);
        }
    }

    /*
     * @param taskDefinition Task Definition to be updated
     */
    @Service
    public void updateTaskDef(TaskDef taskDefinition) {
        TaskDef existing = metadataDAO.getTaskDef(taskDefinition.getName());
        if (existing == null) {
            throw new ApplicationException(Code.NOT_FOUND, "No such task by name " + taskDefinition.getName());
        }
        taskDefinition.setUpdatedBy(WorkflowContext.get().getClientApp());
        taskDefinition.setUpdateTime(System.currentTimeMillis());
        metadataDAO.updateTaskDef(taskDefinition);
    }

    /**
     * @param taskType Remove task definition
     */
    @Service
    public void unregisterTaskDef(String taskType) {
        metadataDAO.removeTaskDef(taskType);
    }

    /**
     * @return List of all the registered tasks
     */
    public List<TaskDef> getTaskDefs() {
        return metadataDAO.getAllTaskDefs();
    }

    /**
     * @param taskType Task to retrieve
     * @return Task Definition
     */
    @Service
    public TaskDef getTaskDef(String taskType) {
        TaskDef taskDef = metadataDAO.getTaskDef(taskType);
        if (taskDef == null) {
            throw new ApplicationException(Code.NOT_FOUND,
                String.format("No such taskType found by name: %s", taskType));
        }
        return taskDef;
    }

    /**
     * @param workflowDef Workflow definition to be updated
     */
    @Service
    public void updateWorkflowDef(WorkflowDef workflowDef) {
        workflowDef.setUpdateTime(System.currentTimeMillis());
        metadataDAO.updateWorkflowDef(workflowDef);
    }

    /**
     * @param workflowDefList Workflow definitions to be updated.
     */
    @Service
    public void updateWorkflowDef(List<WorkflowDef> workflowDefList) {
        for (WorkflowDef workflowDef : workflowDefList) {
            workflowDef.setUpdateTime(System.currentTimeMillis());
            metadataDAO.updateWorkflowDef(workflowDef);
        }
    }

    /**
     * @param name    Name of the workflow to retrieve
     * @param version Optional.  Version.  If null, then retrieves the latest
     * @return Workflow definition
     */
    @Service
    public WorkflowDef getWorkflowDef(String name, Integer version) {
        Optional<WorkflowDef> workflowDef;
        if (version == null) {
            workflowDef = metadataDAO.getLatestWorkflowDef(name);
        } else {
            workflowDef = metadataDAO.getWorkflowDef(name, version);
        }

        return workflowDef.orElseThrow(() -> new ApplicationException(Code.NOT_FOUND,
            String.format("No such workflow found by name: %s, version: %d", name, version)));
    }

    /**
     * @param name Name of the workflow to retrieve
     * @return Latest version of the workflow definition
     */
    @Service
    public Optional<WorkflowDef> getLatestWorkflow(String name) {
        return metadataDAO.getLatestWorkflowDef(name);
    }

    public List<WorkflowDef> getWorkflowDefs() {
        return metadataDAO.getAllWorkflowDefs();
    }

    @Service
    public void registerWorkflowDef(WorkflowDef workflowDef) {
        if (workflowDef.getName().contains(":")) {
            throw new ApplicationException(Code.INVALID_INPUT,
                "Workflow name cannot contain the following set of characters: ':'");
        }
        if (workflowDef.getSchemaVersion() < 1 || workflowDef.getSchemaVersion() > 2) {
            workflowDef.setSchemaVersion(2);
        }
        workflowDef.setCreateTime(System.currentTimeMillis());
        metadataDAO.createWorkflowDef(workflowDef);
    }

    /**
     * @param name    Name of the workflow definition to be removed
     * @param version Version of the workflow definition to be removed
     */
    @Service
    public void unregisterWorkflowDef(String name, Integer version) {
        metadataDAO.removeWorkflowDef(name, version);
    }

    /**
     * @param eventHandler Event handler to be added. Will throw an exception if an event handler already exists with
     *                     the name
     */
    @Service
    public void addEventHandler(EventHandler eventHandler) {
        eventQueues.getQueue(eventHandler.getEvent());
        eventHandlerDAO.addEventHandler(eventHandler);
    }

    /**
     * @param eventHandler Event handler to be updated.
     */
    @Service
    public void updateEventHandler(EventHandler eventHandler) {
        eventQueues.getQueue(eventHandler.getEvent());
        eventHandlerDAO.updateEventHandler(eventHandler);
    }

    /**
     * @param name Removes the event handler from the system
     */
    @Service
    public void removeEventHandlerStatus(String name) {
        eventHandlerDAO.removeEventHandler(name);
    }

    /**
     * @return All the event handlers registered in the system
     */
    public List<EventHandler> getAllEventHandlers() {
        return eventHandlerDAO.getAllEventHandlers();
    }

    /**
     * @param event      name of the event
     * @param activeOnly if true, returns only the active handlers
     * @return Returns the list of all the event handlers for a given event
     */
    @Service
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
        return eventHandlerDAO.getEventHandlersForEvent(event, activeOnly);
    }
}

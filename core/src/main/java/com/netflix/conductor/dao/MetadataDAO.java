/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.dao;

import java.util.List;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

/**
 * @author Viren
 * Data access layer for the workflow metadata - task definitions and workflow definitions
 */
public interface MetadataDAO {

    /**
     *
     * @param taskDef task definition to be created
     * @return name of the task definition
     *
     */
    public abstract String createTaskDef(TaskDef taskDef);

    /**
     *
     * @param taskDef task definition to be updated.
     * @return name of the task definition
     *
     */
    public abstract String updateTaskDef(TaskDef taskDef);

    /**
     *
     * @param name Name of the task
     * @return Task Definition
     *
     */
    public abstract TaskDef getTaskDef(String name);

    /**
     *
     * @return All the task definitions
     *
     */
    public abstract List<TaskDef> getAllTaskDefs();

    /**
     *
     * @param name Name of the task
     */
    public abstract void removeTaskDef(String name);

    /**
     *
     * @param def workflow definition
     *
     */
    public abstract void create(WorkflowDef def);

    /**
     *
     * @param def workflow definition
     *
     */
    public abstract void update(WorkflowDef def);

    /**
     *
     * @param name Name of the workflow
     * @return Workflow Definition
     *
     */
    public abstract WorkflowDef getLatest(String name);

    /**
     *
     * @param name Name of the workflow
     * @param version version
     * @return workflow definition
     *
     */
    public abstract WorkflowDef get(String name, int version);

    /**
     *
     * @return Names of all the workflows
     *
     */
    public abstract List<String> findAll();

    /**
     *
     * @return List of all the workflow definitions
     *
     */
    public abstract List<WorkflowDef> getAll();

    /**
     *
     * @return List of all the latest workflow definitions
     *
     */
    public abstract List<WorkflowDef> getAllLatest();

    /**
     *
     * @param name name of the workflow
     * @return List of all the workflow definitions
     *
     */
    public abstract List<WorkflowDef> getAllVersions(String name);

    /**
     *
     * @param eventHandler Event handler to be added.
     * Will throw an exception if an event handler already exists with the name
     */
    public abstract void addEventHandler(EventHandler eventHandler);

    /**
     *
     * @param eventHandler Event handler to be updated.
     */
    public abstract void updateEventHandler(EventHandler eventHandler);

    /**
     *
     * @param name Removes the event handler from the system
     */
    public abstract void removeEventHandlerStatus(String name);

    /**
     *
     * @return All the event handlers registered in the system
     */
    public List<EventHandler> getEventHandlers();

    /**
     *
     * @param event name of the event
     * @param activeOnly if true, returns only the active handlers
     * @return Returns the list of all the event handlers for a given event
     */
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly);
}

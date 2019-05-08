/**
 * Copyright 2016 Netflix, Inc.
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
/**
 *
 */
package com.netflix.conductor.service;

import com.google.common.base.Preconditions;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

/**
 * @author Viren
 */
@Singleton
@Trace
public class MetadataService {

	private IndexDAO indexer;
	private MetadataDAO metadata;
	private WorkflowExecutor executor;
	private ExecutionService service;

	@Inject
	public MetadataService(MetadataDAO metadata, IndexDAO indexDAO, ExecutionService service, WorkflowExecutor executor) {
		this.metadata = metadata;
		this.service = service;
		this.executor = executor;
		this.indexer = indexDAO;
	}

	/**
	 * @param taskDefs Task Definitions to register
	 */
	public void registerTaskDef(List<TaskDef> taskDefs) {
		for (TaskDef taskDef : taskDefs) {
			taskDef.setCreatedBy(WorkflowContext.get().getClientApp());
			taskDef.setCreateTime(System.currentTimeMillis());
			taskDef.setUpdatedBy(null);
			taskDef.setUpdateTime(null);
			metadata.createTaskDef(taskDef);
		}
	}

	/**
	 * @param taskDef Task Definition to be updated
	 */
	public void updateTaskDef(TaskDef taskDef) {
		TaskDef existing = metadata.getTaskDef(taskDef.getName());
		if (existing == null) {
			throw new ApplicationException(Code.NOT_FOUND, "No such task by name " + taskDef.getName());
		}
		taskDef.setUpdatedBy(WorkflowContext.get().getClientApp());
		taskDef.setUpdateTime(System.currentTimeMillis());
		metadata.updateTaskDef(taskDef);
	}

	/**
	 * @param taskType Remove task definition
	 */
	public void unregisterTaskDef(String taskType) {
		metadata.removeTaskDef(taskType);
	}

	/**
	 * @return List of all the registered tasks
	 */
	public List<TaskDef> getTaskDefs() {
		return metadata.getAllTaskDefs();
	}

	/**
	 * @param taskType Task to retrieve
	 * @return Task Definition
	 */
	public TaskDef getTaskDef(String taskType) {
		return metadata.getTaskDef(taskType);
	}

	/**
	 * @param def Workflow definition to be updated
	 */
	public void updateWorkflowDef(WorkflowDef def) {
		metadata.update(def);
	}

	/**
	 * @param wfs Workflow definitions to be updated.
	 */
	public void updateWorkflowDef(List<WorkflowDef> wfs) {
		for (WorkflowDef wf : wfs) {
			metadata.update(wf);
		}
	}

	/**
	 * @param name    Name of the workflow to retrieve
	 * @param version Optional.  Version.  If null, then retrieves the latest
	 * @return Workflow definition
	 */
	public WorkflowDef getWorkflowDef(String name, Integer version) {
		if (version == null) {
			return metadata.getLatest(name);
		}
		return metadata.get(name, version);
	}

	/**
	 * Remove workflow definition
	 *
	 * @param name    workflow name
	 * @param version workflow version
	 */
	public void unregisterWorkflow(String name, Integer version) {
		WorkflowDef def;
		if (version == null) {
			def = metadata.getLatest(name);
		} else {
			def = metadata.get(name, version);
		}

		if (def == null) {
			throw new ApplicationException(Code.NOT_FOUND, "No such workflow by name " + name + " and version " + version);
		}

		String query = "workflowType IN (" + def.getName() + ") AND version IN (" + def.getVersion() + ")";

		// Work in the batch mode
		while (true) {
			// Get the batch
			SearchResult<WorkflowSummary> workflows = service.search(query, "*", 0, 100, null, null, null);
			if (workflows.getTotalHits() <= 0) {
				break;
			}

			// Process batch
			workflows.getResults().forEach(workflow -> {
				// Terminate workflow if it is running
				if (Workflow.WorkflowStatus.RUNNING == workflow.getStatus()) {
					try {
						executor.terminateWorkflow(workflow.getWorkflowId(), "Metadata deleting requested");
					} catch (Exception ignore) {
					}
				}

				// remove workflow
				try {
					service.removeWorkflow(workflow.getWorkflowId());
				} catch (Exception ignore) {
				}

				// remove from index
				try {
					indexer.remove(workflow.getWorkflowId());
				} catch (Exception ignore) {
				}
			});
		}

		// remove metadata
		metadata.removeWorkflow(def);
	}

	/**
	 * @param name Name of the workflow to retrieve
	 * @return Latest version of the workflow definition
	 */
	public WorkflowDef getLatestWorkflow(String name) {
		return metadata.getLatest(name);
	}

	public List<WorkflowDef> getWorkflowDefs() {
		return metadata.getAll();
	}

	public void registerWorkflowDef(WorkflowDef def) {
		if (def.getName().contains(":")) {
			throw new ApplicationException(Code.INVALID_INPUT, "Workflow name cannot contain the following set of characters: ':'");
		}
		if (def.getSchemaVersion() < 1 || def.getSchemaVersion() > 2) {
			def.setSchemaVersion(2);
		}
		metadata.create(def);
	}

	/**
	 * @param eventHandler Event handler to be added.
	 *                     Will throw an exception if an event handler already exists with the name
	 */
	public void addEventHandler(EventHandler eventHandler) {
		validateEvent(eventHandler);
		EventHandler existing = getEventHandler(eventHandler.getName());
		if (existing != null) {
			throw new ApplicationException(ApplicationException.Code.CONFLICT, "EventHandler with name " + eventHandler.getName() + " already exists!");
		}
		validateQueue(eventHandler);
		metadata.addEventHandler(eventHandler);
	}

	/**
	 * @param eventHandler Event handler to be updated.
	 */
	public void updateEventHandler(EventHandler eventHandler) {
		validateEvent(eventHandler);
		EventHandler existing = getEventHandler(eventHandler.getName());
		if (existing == null) {
			throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "EventHandler with name " + eventHandler.getName() + " not found!");
		}

		// Are subject:queue the same or been updated ?
		boolean eventEquals = existing.getEvent().equalsIgnoreCase(eventHandler.getEvent());

		// The new handler is disabled - close old queue
		if (existing.isActive() && !eventHandler.isActive()) {
			EventQueues.remove(existing.getEvent());
		} else if (!eventEquals) { // if subject:queue was updated
			EventQueues.remove(existing.getEvent());
		}

		validateQueue(eventHandler);
		metadata.updateEventHandler(eventHandler);
	}

	/**
	 * @param name Removes the event handler from the system
	 */
	public void removeEventHandlerStatus(String name) {
		EventHandler existing = getEventHandler(name);
		if (existing == null) {
			throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "EventHandler with name " + name + " not found!");
		}
		EventQueues.remove(existing.getEvent());
		metadata.removeEventHandlerStatus(name);
	}

	/**
	 * @return All the event handlers registered in the system
	 */
	public List<EventHandler> getEventHandlers() {
		return metadata.getEventHandlers();
	}

	/**
	 * @param event      name of the event
	 * @param activeOnly if true, returns only the active handlers
	 * @return Returns the list of all the event handlers for a given event
	 */
	public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
		return metadata.getEventHandlersForEvent(event, activeOnly);
	}

	/**
	 * This method just validates required fields
	 *
	 * @param eh Event handler definition
	 */
	private void validateEvent(EventHandler eh) {
		Preconditions.checkNotNull(eh.getName(), "Missing event handler name");
		Preconditions.checkNotNull(eh.getEvent(), "Missing event location");
		Preconditions.checkNotNull(eh.getActions(), "No actions specified.  Please specify at-least one action");
	}

	/**
	 * This method was created to divide event handler validation logic and the queue validation.
	 * We do not want to register queue in the EventQueues if existence logic fails
	 *
	 * @param eh Event handler definition
	 */
	private void validateQueue(EventHandler eh) {
		String event = eh.getEvent();
		if (eh.isActive()) {
			EventQueues.getQueue(event, true);
		}
	}

	private EventHandler getEventHandler(String name) {
		return getEventHandlers().stream()
				.filter(eh -> eh.getName().equalsIgnoreCase(name))
				.findFirst().orElse(null);
	}
}

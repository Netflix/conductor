/*
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
package com.netflix.conductor.service;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Viren
 *
 */
@Singleton
public class WorkflowMonitor {
	private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowMonitor.class);

	private final MetadataDAO metadataDAO;
	private final QueueDAO queueDAO;
	private final ExecutionDAOFacade executionDAOFacade;

	private ScheduledExecutorService scheduledExecutorService;

	private List<TaskDef> taskDefs;
	private List<WorkflowDef> workflowDefs;

	private int refreshCounter = 0;
	private int metadataRefreshInterval;
	private int statsFrequencyInSeconds;

	@Inject
	public WorkflowMonitor(MetadataDAO metadataDAO, QueueDAO queueDAO, ExecutionDAOFacade executionDAOFacade, Configuration config) {
		this.metadataDAO = metadataDAO;
		this.queueDAO = queueDAO;
		this.executionDAOFacade = executionDAOFacade;
		this.metadataRefreshInterval = config.getIntProperty("workflow.monitor.metadata.refresh.counter", 10);
		this.statsFrequencyInSeconds = config.getIntProperty("workflow.monitor.stats.freq.seconds", 60);
		init();
	}

	public void init() {
		this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
		this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
			try {
				if (refreshCounter <= 0) {
					workflowDefs = metadataDAO.getAll();
					taskDefs = new ArrayList<>(metadataDAO.getAllTaskDefs());
					refreshCounter = metadataRefreshInterval;
				}

				workflowDefs.forEach(workflowDef -> {
					String name = workflowDef.getName();
					String version = String.valueOf(workflowDef.getVersion());
					String ownerApp = workflowDef.getOwnerApp();
					long count = executionDAOFacade.getPendingWorkflowCount(name);
					Monitors.recordRunningWorkflows(count, name, version, ownerApp);
				});

				taskDefs.forEach(taskDef -> {
					String taskIsolatedQueue = QueueUtils.getQueueName(taskDef.getName(),null,taskDef.getIsolationGroupId(), taskDef.getExecutionNameSpace());
					long size = queueDAO.getSize(taskDef.getName());
					long inProgressCount = executionDAOFacade.getInProgressTaskCount(taskDef.getName());
					Monitors.recordQueueDepth(taskIsolatedQueue, queueDAO.getSize(taskIsolatedQueue), taskDef.getOwnerApp());
					Monitors.recordQueueDepth(taskDef.getName(), size, taskDef.getOwnerApp());
					if(taskDef.concurrencyLimit() > 0) {
						Monitors.recordTaskInProgress(taskDef.getName(), inProgressCount, taskDef.getOwnerApp());
					}
				});

				refreshCounter--;
			} catch (Exception e) {
				LOGGER.error("Error while publishing scheduled metrics", e);
			}
		}, 120, statsFrequencyInSeconds, TimeUnit.SECONDS);
	}
}

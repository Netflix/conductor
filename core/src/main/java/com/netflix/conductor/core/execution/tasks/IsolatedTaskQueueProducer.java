package com.netflix.conductor.core.execution.tasks;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class IsolatedTaskQueueProducer {

	private static Logger logger = LoggerFactory.getLogger(IsolatedTaskQueueProducer.class);
	private MetadataService metadataService;
	private Configuration config;
	private int pollingTimeOut;


	@Inject
	public IsolatedTaskQueueProducer(MetadataService metadataService, Configuration configuration) {
		this.metadataService = metadataService;
		this.config = configuration;
		this.pollingTimeOut = config.getIntProperty("workflow.isolated.system.task.poll.time.secs", 10);

		boolean listenForIsolationGroups = config.getBooleanProperty("workflow.isolated.system.task.enable", false);

		if (listenForIsolationGroups) {

			logger.info("Listening for isolation groups");
			new Thread(this::syncTaskQueues).start();

		} else {

			logger.info("Isolated System Task Worker DISABLED");

		}

	}



	void syncTaskQueues() {
		try {

			for (; !Thread.currentThread().isInterrupted(); ) {
				addTaskQueues();
				TimeUnit.SECONDS.sleep(pollingTimeOut);
			}

		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			logger.info("Received interrupt - returning", ie);
		}
	}

	private Set<TaskDef> getIsolationDomain() throws InterruptedException {

		Set<TaskDef> isolationDomainGroups = Collections.emptySet();

		try {

			List<TaskDef> taskDefs = metadataService.getTaskDefs();
			isolationDomainGroups = taskDefs.stream().
					filter(taskDef -> StringUtils.isNotBlank(taskDef.getIsolationGroupId())|| StringUtils.isNotBlank(taskDef.getDomain())).
					collect(Collectors.toSet());

		} catch (RuntimeException unknownException) {

			logger.error("Unknown exception received in getting isolation groups, sleeping and retrying", unknownException);
			TimeUnit.SECONDS.sleep(pollingTimeOut);

		}
		return isolationDomainGroups;
	}

	@VisibleForTesting
	void addTaskQueues() throws InterruptedException {

		Set<TaskDef> isolationDefs = getIsolationDomain();
		logger.debug("Retrieved queues {}", isolationDefs);
		Set<String> taskTypes = SystemTaskWorkerCoordinator.taskNameWorkFlowTaskMapping.keySet();

		for (TaskDef isolatedTaskDef : isolationDefs) {
			for (String taskType : taskTypes) {
				String taskQueue = QueueUtils.getQueueName(taskType, isolatedTaskDef.getDomain(), isolatedTaskDef.getIsolationGroupId());
				logger.debug("Adding task={} to coordinator queue", taskQueue);
				SystemTaskWorkerCoordinator.queue.add(taskQueue);

			}
		}

	}

}

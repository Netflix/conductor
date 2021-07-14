package com.netflix.conductor.server;

import com.netflix.conductor.core.events.EventProcessor;
import com.netflix.conductor.core.execution.WorkflowSweeper;
import com.netflix.conductor.core.execution.batch.BatchSweeper;
import com.netflix.conductor.core.execution.tasks.SystemTaskWorkerCoordinator;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

@Singleton
public class ServerShutdown {
	private static final Logger logger = LoggerFactory.getLogger(ServerShutdown.class);
	private final SystemTaskWorkerCoordinator taskWorkerCoordinator;
	private final WorkflowSweeper workflowSweeper;
	private final EventProcessor eventProcessor;
	private final BatchSweeper batchSweeper;
	private final DataSource dataSource;

	@Inject
	public ServerShutdown(SystemTaskWorkerCoordinator taskWorkerCoordinator,
						  WorkflowSweeper workflowSweeper,
						  EventProcessor eventProcessor,
						  BatchSweeper batchSweeper,
						  DataSource dataSource) {
		this.taskWorkerCoordinator = taskWorkerCoordinator;
		this.workflowSweeper = workflowSweeper;
		this.eventProcessor = eventProcessor;
		this.batchSweeper = batchSweeper;
		this.dataSource = dataSource;

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				shutdown();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}));
	}

	private void shutdown() {
		batchSweeper.shutdown();
		eventProcessor.shutdown();
		workflowSweeper.shutdown();
		taskWorkerCoordinator.shutdown();

		logger.info("Closing primary data source");
		if (dataSource instanceof HikariDataSource) {
			((HikariDataSource) dataSource).close();
		}
	}
}

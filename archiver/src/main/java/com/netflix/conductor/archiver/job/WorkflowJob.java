package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class WorkflowJob extends AbstractJob {
	private static final Logger logger = LogManager.getLogger(WorkflowJob.class);
	private BlockingDeque<String> workflowQueue = new LinkedBlockingDeque<>();
	private AtomicBoolean keepPooling = new AtomicBoolean(true);
	private final AppConfig config = AppConfig.getInstance();
	private CountDownLatch latch = new CountDownLatch(config.queueWorkers());
	private Set<String> processed = ConcurrentHashMap.newKeySet();
	private long endTime;

	public WorkflowJob(HikariDataSource dataSource) {
		super(dataSource);
	}

	@Override
	public void cleanup() {
		endTime = System.currentTimeMillis() - Duration.ofDays(config.keepDays()).toMillis();
		logger.info("Starting with keepDays " + config.keepDays() + ", endTime " + new Timestamp(endTime));

		// Start workers
		startWorkers();

		// Grab root level workflows
		grabWorkflows();

		// Wait until processed
		waitWorkflows();
	}

	private void startWorkers() {
		Runnable runnable = () -> {
			while (keepPooling.get() || !workflowQueue.isEmpty()) {
				String workflowId = workflowQueue.poll();
				if (workflowId != null) {
					try {
						try (Connection tx = dataSource.getConnection()) {
							tx.setAutoCommit(false);
							try {

								processWorkflow(workflowId, tx);
								tx.commit();

							} catch (Throwable th) {
								tx.rollback();
								throw th;
							}
						}
					} catch (Throwable th) {
						logger.error(th.getMessage() + " occurred for " + workflowId, th);
					}
				} else {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			logger.info("No workflows left to process. Finishing " + Thread.currentThread().getName());
			latch.countDown();
		};

		IntStream.range(0, config.queueWorkers()).forEach(o -> {
			Thread thread = new Thread(runnable);
			thread.setName("worker-" + o);
			thread.start();
		});
	}

	private void grabWorkflows() {
		String SQL = "SELECT workflow_id FROM workflow WHERE end_time < ? " +
			"AND workflow_status NOT IN ('RESET', 'RUNNING', 'PAUSED') AND parent_workflow_id IS NULL";
		try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(SQL)) {
			st.setTimestamp(1, new Timestamp(endTime));
			ResultSet rs = st.executeQuery();
			int totalHits = 0;
			while (rs.next()) {
				String workflowId = rs.getString("workflow_id");
				workflowQueue.add(workflowId);
				totalHits++;
			}
			logger.info("Found " + totalHits + " root level workflows to be deleted");
		} catch (Exception ex) {
			keepPooling.set(false);
			logger.error("grabWorkflows failed with " + ex.getMessage(), ex);
			throw new RuntimeException(ex.getMessage(), ex);
		}
	}

	private void waitWorkflows() {
		int size;
		while ((size = workflowQueue.size()) > 0) {
			logger.info("Waiting ... workflows left " + size);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		keepPooling.set(false);
		logger.info("Waiting for workers to complete");
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void processWorkflow(String workflowId, Connection tx) throws SQLException {
		if (processed.contains(workflowId)) {
			logger.debug("Workflow " + workflowId + " already processed");
			return;
		}
		executeUpdate(tx, "DELETE FROM task WHERE workflow_id = ?", workflowId);
		executeUpdate(tx, "DELETE FROM task_scheduled WHERE workflow_id = ?", workflowId);
		executeUpdate(tx, "DELETE FROM task_in_progress WHERE workflow_id = ?", workflowId);
		executeUpdate(tx, "DELETE FROM workflow WHERE workflow_id = ?", workflowId);

		processChildren(workflowId, tx);

		processed.add(workflowId);
	}


	private void processChildren(String workflowId, Connection tx) throws SQLException {
		try (PreparedStatement st = tx.prepareStatement("SELECT workflow_id FROM workflow WHERE parent_workflow_id = ?")) {
			st.setString(1, workflowId);
			ResultSet rs = st.executeQuery();
			while (rs.next()) {
				workflowId = rs.getString("workflow_id");
				processWorkflow(workflowId, tx);
			}
		}
	}

	private void executeUpdate(Connection tx, String query, String workflowId) throws SQLException {
		try (PreparedStatement st = tx.prepareStatement(query)) {
			st.setString(1, workflowId);
			st.executeUpdate();
		}
	}
}

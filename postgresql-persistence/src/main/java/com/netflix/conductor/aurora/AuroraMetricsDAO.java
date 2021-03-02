package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.aurora.sql.ResultSetHandler;

import com.netflix.conductor.dao.MetricsDAO;
import com.netflix.conductor.service.MetricService;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AuroraMetricsDAO extends AuroraBaseDAO implements MetricsDAO {

	@Inject
	public AuroraMetricsDAO(DataSource dataSource, ObjectMapper mapper) {
		super(dataSource, mapper);
	}

	@Override
	public boolean ping() {
		return queryWithTransaction("select true", q -> q.executeScalar(Boolean.class));
	}

	@Override
	public Map<String, Object> getMetrics() {
		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// Admin counters
			futures.add(pool.submit(this::adminCounters));

			// Workflow count
			futures.add(pool.submit(this::workflowCount));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return Collections.emptyMap();
	}

	// Wait until all futures completed
	private void waitCompleted(List<Future<?>> futures) {
		for (Future<?> future : futures) {
			try {
				if (future != null) {
					future.get();
				}
			} catch (Exception ex) {
				logger.error("Get future failed " + ex.getMessage(), ex);
			}
		}
	}

	private void adminCounters() {
		ResultSetHandler<Object> handler = rs -> {
			while (rs.next()) {
				String queueName = rs.getString("queue_name").toLowerCase();
				Long count = rs.getLong("count");
				MetricService.getInstance().queueGauge(queueName, count);
			}
			return null;
		};

		String SQL = "select queue_name, count(*) as count from queue_message group by queue_name";
		queryWithTransaction(SQL, q -> q.executeAndFetch(handler));
	}

	private void workflowCount() {
		ResultSetHandler<Object> handler = rs -> {
			while (rs.next()) {
				String workflowType = rs.getString("workflow_type").toLowerCase();
				Long count = rs.getLong("count");
				MetricService.getInstance().workflowGauge(workflowType, count);
			}
			return null;
		};

		String SQL = "select workflow_type, count(*) as count from workflow where workflow_status = 'RUNNING' group by workflow_type";
		queryWithTransaction(SQL, q -> q.executeAndFetch(handler));
	}

	@Override
	public List<String> getStuckChecksums(Long startTime, Long endTime) {
		String SQL = "SELECT t2.output::jsonb->'response'->'body'->>'DispatchedJobID' AS jobId, " +
			"'http://conductor-ui.service.owf-int/#/workflow/id/' || w.workflow_id AS workflow, " +
			"t2.created_on " +
			"FROM workflow w, task t2 " +
			"WHERE w.workflow_id = t2.workflow_id " +
			"AND w.workflow_status = 'RUNNING' " +
			"AND t2.created_on BETWEEN ? AND ? " +
			"AND t2.task_refname = 'getChecksum' " +
			"AND t2.task_status = 'COMPLETED'";

		return queryWithTransaction(SQL, q -> q.addTimestampParameter(startTime)
			.addTimestampParameter(endTime)
			.executeAndFetch(resultSet -> {
				List<String> result = new ArrayList<>();
				while (resultSet.next()) {
					result.add("{" +
						"'jobId':'" + resultSet.getString("jobId") + "'," +
						"'workflow':'" + resultSet.getString("workflow") + "'," +
						"'createdOn':'" + resultSet.getString("created_on") + "'" +
						"}");
				}
				return result;
			}));
	}

}

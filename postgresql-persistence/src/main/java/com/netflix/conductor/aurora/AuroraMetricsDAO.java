package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.dao.MetricsDAO;
import com.netflix.conductor.service.MetricService;
import org.apache.commons.lang3.tuple.Pair;

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

			futures.add(pool.submit(this::queueDepth));
			futures.add(pool.submit(this::httpQueueDepth));
			futures.add(pool.submit(this::deciderQueueDepth));
			futures.add(pool.submit(this::httpRunning));

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

	private void queueDepth() {
		String SQL = "SELECT queue_name, count(*) as count FROM queue_message GROUP BY queue_name";
		List<Pair<String, Long>> queueData = queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
			List<Pair<String, Long>> result = new LinkedList<>();
			while (rs.next()) {
				String queueName = rs.getString("queue_name").toLowerCase();
				Long count = rs.getLong("count");
				result.add(Pair.of(queueName, count));
			}
			return result;
		}));
		queueData.forEach(p -> MetricService.getInstance().queueDepth(p.getKey(), p.getValue()));
	}

	private void httpQueueDepth() {
		String SQL = "SELECT t.task_refname as task_refname " +
				", t.json_data::jsonb->>'taskDefName' as task_defname " +
				", (CASE WHEN (t.input::jsonb->'http_request')::jsonb->>'serviceDiscoveryQuery' IS NOT NULL THEN (t.input::jsonb->'http_request')::jsonb->>'serviceDiscoveryQuery' " +
				"  WHEN (t.input::jsonb->'http_request')::jsonb->>'uri' LIKE 'https://%' THEN substr(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','https://'),0,strpos(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','https://'),'/')) " +
				"  WHEN (t.input::jsonb->'http_request')::jsonb->>'uri' LIKE 'http://%' THEN substr(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','http://'),0,strpos(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','http://'),':')) " +
				"  END) as service_name " +
				", count(*) " +
				"FROM task t JOIN queue_message q ON t.task_id = q.message_id " +
				"WHERE q.queue_name = 'http' " +
				"GROUP BY 1,2,3";
		List<Pair<Pair<String, String>, Pair<String, Long>>> httpData = queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
			List<Pair<Pair<String, String>, Pair<String, Long>>> result = new LinkedList<>();
			while (rs.next()) {
				String taskRefName = rs.getString("task_refname");
				String taskDefName = rs.getString("task_defname");
				String serviceName = rs.getString("service_name");
				Long count = rs.getLong("count");

				result.add(Pair.of(Pair.of(taskRefName, taskDefName), Pair.of(serviceName, count)));
			}
			return result;
		}));
		httpData.forEach(p -> MetricService.getInstance().httpQueueDepth(
				p.getKey().getLeft(),      // task ref name
				p.getKey().getRight(),     // task def name
				p.getValue().getLeft(),    // service name
				p.getValue().getRight())); // count
	}

	private void deciderQueueDepth() {
		String SQL = "SELECT w.workflow_type, count(*) as count " +
				"FROM queue_message q JOIN workflow w ON w.workflow_id = q.message_id " +
				"WHERE q.queue_name = '_deciderqueue' " +
				"GROUP BY w.workflow_type";
		List<Pair<String, Long>> wfData = queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
			List<Pair<String, Long>> result = new LinkedList<>();
			while (rs.next()) {
				String workflowType = rs.getString("workflow_type").toLowerCase();
				Long count = rs.getLong("count");
				result.add(Pair.of(workflowType, count));
			}
			return result;
		}));
		wfData.forEach(p -> MetricService.getInstance().deciderQueueDepth(p.getKey(), p.getValue()));
	}

	private void httpRunning() {
		String SQL = "SELECT t.task_refname as task_refname " +
				", t.json_data::jsonb->>'taskDefName' as task_defname " +
				",(CASE WHEN (t.input::jsonb->'http_request')::jsonb->>'serviceDiscoveryQuery' IS NOT NULL THEN (t.input::jsonb->'http_request')::jsonb->>'serviceDiscoveryQuery' " +
				"  WHEN (t.input::jsonb->'http_request')::jsonb->>'uri' LIKE 'https://%' THEN substr(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','https://'),0,strpos(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','https://'),'/')) " +
				"  WHEN (t.input::jsonb->'http_request')::jsonb->>'uri' LIKE 'http://%' THEN substr(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','http://'),0,strpos(ltrim((t.input::jsonb->'http_request')::jsonb->>'uri','http://'),':')) " +
				"  END) as service_name\n" +
				", count(*)\n" +
				"FROM task t JOIN task_in_progress p ON t.task_id = p.task_id\n" +
				"WHERE p.in_progress = true\n" +
				"GROUP BY 1, 2, 3";
		List<Pair<Pair<String, String>, Pair<String, Long>>> httpData = queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
			List<Pair<Pair<String, String>, Pair<String, Long>>> result = new LinkedList<>();
			while (rs.next()) {
				String taskRefName = rs.getString("task_refname");
				String taskDefName = rs.getString("task_defname");
				String serviceName = rs.getString("service_name");
				Long count = rs.getLong("count");

				result.add(Pair.of(Pair.of(taskRefName, taskDefName), Pair.of(serviceName, count)));
			}
			return result;
		}));
		httpData.forEach(p -> MetricService.getInstance().httpRunningGauge(
				p.getKey().getLeft(),      // task ref name
				p.getKey().getRight(),     // task def name
				p.getValue().getLeft(),    // service name
				p.getValue().getRight())); // count
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

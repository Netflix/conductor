package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.aurora.sql.ResultSetHandler;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.MetricsDAO;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class AuroraMetricsDAO extends AuroraBaseDAO implements MetricsDAO {
	private static final List<String> WORKFLOW_TODAY_STATUSES = Arrays.asList(
		Workflow.WorkflowStatus.COMPLETED.name(),
		Workflow.WorkflowStatus.CANCELLED.name(),
		Workflow.WorkflowStatus.TIMED_OUT.name(),
		Workflow.WorkflowStatus.RUNNING.name(),
		Workflow.WorkflowStatus.FAILED.name()
	);
	private static final List<String> WORKFLOW_OVERALL_STATUSES = Arrays.asList(
		Workflow.WorkflowStatus.COMPLETED.name(),
		Workflow.WorkflowStatus.CANCELLED.name(),
		Workflow.WorkflowStatus.TIMED_OUT.name(),
		Workflow.WorkflowStatus.FAILED.name()
	);

	private static final List<String> TASK_TYPES = Arrays.asList(
		"WAIT",
		"HTTP",
		"BATCH");

	private static final List<String> TASK_STATUSES = Arrays.asList(
		Task.Status.IN_PROGRESS.name(),
		Task.Status.COMPLETED.name(),
		Task.Status.FAILED.name()
	);

	private static final List<String> EVENT_STATUSES = Arrays.asList(
		EventExecution.Status.COMPLETED.name(),
		EventExecution.Status.SKIPPED.name(),
		EventExecution.Status.FAILED.name()
	);

	private static final List<String> SINK_SUBJECTS = Arrays.asList(
		"deluxe.conductor.deluxeone.compliance.workflow.update",
		"deluxe.conductor.deluxeone.workflow.update",
		"deluxe.conductor.workflow.update"
	);

	private static final List<String> WORKFLOWS = Arrays.asList(
		"deluxe.dependencygraph.conformancegroup.delivery.process", // Conformance Group
		"deluxe.dependencygraph.assembly.conformancegroup.process", // Sherlock V1 Assembly Conformance
		"deluxe.dependencygraph.sourcewait.process",                // Sherlock V2 Sourcewait
		"deluxe.dependencygraph.execute.process",                   // Sherlock V2 Execute
		"deluxe.deluxeone.sky.compliance.process",                  // Sky Compliance
		"deluxe.delivery.itune.process"                             // iTune
	);

	private static final String VERSION = "\\.\\d+\\.\\d+"; // covers '.X.Y' where X and Y any number/digit
	private static final String PREFIX = "deluxe.conductor";
	private static ParametersUtils pu = new ParametersUtils();
	private MetadataDAO metadataDAO;

	@Inject
	public AuroraMetricsDAO(DataSource dataSource, ObjectMapper mapper, MetadataDAO metadataDAO, Configuration config) {
		super(dataSource, mapper);
		this.metadataDAO = metadataDAO;
	}

	@Override
	public boolean ping() {
		return queryWithTransaction("select true", q -> q.executeScalar(Boolean.class));
	}

	@Override
	public Map<String, Object> getMetrics() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Main workflow list
		Set<String> fullNames = getMainWorkflows();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today + overall
			for (boolean today : Arrays.asList(true, false)) {

				// then per each short name
				for (String shortName : WORKFLOWS) {
					// Filter workflow definitions to have current short related only
					Set<String> filtered = fullNames.stream().filter(type -> type.startsWith(shortName)).collect(Collectors.toSet());

					// Workflow counter per short name
					futures.add(pool.submit(() -> workflowCounters(metrics, today, shortName, filtered)));

					// Workflow average per short name
					futures.add(pool.submit(() -> workflowAverage(metrics, today, shortName, filtered)));
				}

				// Task type/refName/status counter
				futures.add(pool.submit(() -> taskTypeRefNameCounters(metrics, today)));

				// Task type/refName average
				futures.add(pool.submit(() -> taskTypeRefNameAverage(metrics, today)));

				// Event received
				futures.add(pool.submit(() -> eventReceived(metrics, today)));

				// Event published
				futures.add(pool.submit(() -> eventPublished(metrics, today)));

				// Event execution
				futures.add(pool.submit(() -> eventExecAverage(metrics, today)));

				// Event wait
				futures.add(pool.submit(() -> eventWaitAverage(metrics, today)));
			}

			// Admin counters
			futures.add(pool.submit(() -> adminCounters(metrics)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getAdminCounters() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
		adminCounters(metrics);
		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getEventReceived() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today
			futures.add(pool.submit(() -> eventReceived(metrics, true)));

			// overall
			futures.add(pool.submit(() -> eventReceived(metrics, false)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getEventPublished() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today
			futures.add(pool.submit(() -> eventPublished(metrics, true)));

			// overall
			futures.add(pool.submit(() -> eventPublished(metrics, false)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getEventExecAverage() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today
			futures.add(pool.submit(() -> eventExecAverage(metrics, true)));

			// overall
			futures.add(pool.submit(() -> eventExecAverage(metrics, false)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getEventWaitAverage() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today
			futures.add(pool.submit(() -> eventWaitAverage(metrics, true)));

			// overall
			futures.add(pool.submit(() -> eventWaitAverage(metrics, false)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getTaskCounters() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today
			futures.add(pool.submit(() -> taskTypeRefNameCounters(metrics, true)));

			// overall
			futures.add(pool.submit(() -> taskTypeRefNameCounters(metrics, false)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getTaskAverage() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today
			futures.add(pool.submit(() -> taskTypeRefNameAverage(metrics, true)));

			// overall
			futures.add(pool.submit(() -> taskTypeRefNameAverage(metrics, false)));

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getWorkflowCounters() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Main workflow list
		Set<String> fullNames = getMainWorkflows();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today + overall
			for (boolean today : Arrays.asList(true, false)) {

				// then per each short name
				for (String shortName : WORKFLOWS) {
					// Filter workflow definitions to have current short related only
					Set<String> filtered = fullNames.stream().filter(type -> type.startsWith(shortName)).collect(Collectors.toSet());

					// Workflow counter per short name
					futures.add(pool.submit(() -> workflowCounters(metrics, today, shortName, filtered)));
				}
			}

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getWorkflowAverage() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();

		// Main workflow list
		Set<String> fullNames = getMainWorkflows();

		// Using ExecutorService to process in parallel
		ExecutorService pool = Executors.newCachedThreadPool();
		try {
			List<Future<?>> futures = new LinkedList<>();

			// today + overall
			for (boolean today : Arrays.asList(true, false)) {

				// then per each short name
				for (String shortName : WORKFLOWS) {
					// Filter workflow definitions to have current short related only
					Set<String> filtered = fullNames.stream().filter(type -> type.startsWith(shortName)).collect(Collectors.toSet());

					// Workflow average per short name
					futures.add(pool.submit(() -> workflowAverage(metrics, today, shortName, filtered)));
				}
			}

			// Wait until completed
			waitCompleted(futures);
		} finally {
			pool.shutdown();
		}

		return new HashMap<>(metrics);
	}

	// Filters definitions excluding sub workflows and maintenance workflows
	private Set<String> getMainWorkflows() {
		return metadataDAO.findAll().stream()
			.filter(fullName -> WORKFLOWS.stream().anyMatch(shortName -> fullName.matches(shortName + VERSION)))
			.collect(Collectors.toSet());
	}

	private Set<EventHandler> getHandlers() {
		return metadataDAO.getEventHandlers().stream()
			.filter(EventHandler::isActive).collect(Collectors.toSet());
	}

	// Extracts subject name from each active handler
	private Set<String> getSubjects() {
		return getHandlers().stream()
			.map(eh -> eh.getEvent().split(":")[1]) // 0 - event bus, 1 - subject, 2 - queue
			.map(event -> pu.replace(event).toString())
			.collect(Collectors.toSet());
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

	private AtomicLong initMetric(Map<String, AtomicLong> map, String metricName) {
		return map.computeIfAbsent(metricName, s -> new AtomicLong(0));
	}

	private String toLabel(boolean today) {
		return today ? "_today" : "";
	}

	private long getStartTime() {
		TimeZone pst = TimeZone.getTimeZone("UTC");

		Calendar calendar = new GregorianCalendar();
		calendar.setTimeZone(pst);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		return calendar.getTimeInMillis();
	}

	private void adminCounters(Map<String, AtomicLong> map) {
		Set<EventHandler> handlers = getHandlers();
		initMetric(map, String.format("%s.admin.active_handlers", PREFIX)).set(handlers.size());

		String SQL = "select count(*) from queue_message where queue_name = ?";
		long count = queryWithTransaction(SQL, q -> q.addParameter("_deciderqueue").executeScalar(Long.class));

		initMetric(map, String.format("%s.admin.decider_queue", PREFIX)).set(count);
	}

	private void eventReceived(Map<String, AtomicLong> map, boolean today) {
		Set<String> subjects = getSubjects();

		// Init all metrics with 0
		for (String subject : subjects) {
			initMetric(map, String.format("%s.event_received%s.%s", PREFIX, toLabel(today), subject.toLowerCase()));

			for (String status : EVENT_STATUSES) {
				initMetric(map, String.format("%s.event_%s%s.%s", PREFIX, status.toLowerCase(), toLabel(today), subject.toLowerCase()));
			}
		}

		// Per subject metrics
		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String subject = rs.getString("subject").toLowerCase();
					long count = rs.getLong("count");

					String metricName = String.format("%s.event_received%s.%s", PREFIX, toLabel(today), subject);
					map.get(metricName).set(count);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT subject, count(distinct message_id) as count FROM event_execution ");
			SQL.append("WHERE subject = ANY(?) AND status = ANY(?) ");
			if (today) {
				SQL.append("AND received_on >= ? ");
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(EVENT_STATUSES)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(EVENT_STATUSES)
					.executeAndFetch(handler));
			}
		});

		// Per subject/status metrics
		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String subject = rs.getString("subject").toLowerCase();
					String status = rs.getString("status").toLowerCase();
					long count = rs.getLong("count");

					String metricName = String.format("%s.event_%s%s.%s", PREFIX, status, toLabel(today), subject);
					map.get(metricName).set(count);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT subject, status, count(distinct message_id) as count FROM event_execution ");
			SQL.append("WHERE subject = ANY(?) AND status = ANY(?) ");
			if (today) {
				SQL.append("AND received_on >= ? ");
				SQL.append("GROUP BY subject, status");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(EVENT_STATUSES)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY subject, status");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(EVENT_STATUSES)
					.executeAndFetch(handler));
			}
		});
	}

	private void eventExecAverage(Map<String, AtomicLong> map, boolean today) {
		Set<String> subjects = getSubjects();
		List<String> statuses = Arrays.asList("COMPLETED", "FAILED");

		for (String subject : subjects) {
			initMetric(map, String.format("%s.avg_event_exec_msec%s.%s", PREFIX, toLabel(today), subject.toLowerCase()));
		}

		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String subject = rs.getString("subject").toLowerCase();
					long avg = rs.getLong("avg");

					String metricName = String.format("%s.avg_event_exec_msec%s.%s", PREFIX, toLabel(today), subject.toLowerCase());
					map.get(metricName).set(avg);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT subject, avg(extract('epoch' from processed_on) - extract('epoch' from started_on)) as avg ");
			SQL.append("FROM event_execution  ");
			SQL.append("WHERE processed_on IS NOT NULL AND started_on IS NOT NULL ");
			SQL.append("AND subject = ANY(?) AND status = ANY(?) ");
			if (today) {
				SQL.append("AND received_on >= ? ");
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(statuses)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(statuses)
					.executeAndFetch(handler));
			}
		});
	}

	private void eventPublished(Map<String, AtomicLong> map, boolean today) {
		for (String subject : SINK_SUBJECTS) {
			initMetric(map, String.format("%s.event_published%s.%s", PREFIX, toLabel(today), subject.toLowerCase()));
		}

		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String subject = rs.getString("subject").toLowerCase();
					long count = rs.getLong("count");

					String metricName = String.format("%s.event_published%s.%s", PREFIX, toLabel(today), subject.toLowerCase());
					map.get(metricName).set(count);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT subject, count(*) as count FROM event_published ");
			SQL.append("WHERE subject = ANY(?) ");
			if (today) {
				SQL.append("AND published_on >= ? ");
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(SINK_SUBJECTS)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(SINK_SUBJECTS).executeAndFetch(handler));
			}
		});
	}

	private void eventWaitAverage(Map<String, AtomicLong> map, boolean today) {
		Set<String> subjects = getSubjects();

		for (String subject : subjects) {
			initMetric(map, String.format("%s.avg_event_wait_msec%s.%s", PREFIX, toLabel(today), subject.toLowerCase()));
		}

		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String subject = rs.getString("subject").toLowerCase();
					long avg = rs.getLong("avg");

					String metricName = String.format("%s.avg_event_wait_msec%s.%s", PREFIX, toLabel(today), subject.toLowerCase());
					map.get(metricName).set(avg);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT subject, avg(extract('epoch' from accepted_on) - extract('epoch' from received_on)) as avg ");
			SQL.append("FROM event_execution  ");
			SQL.append("WHERE accepted_on IS NOT NULL AND received_on IS NOT NULL ");
			SQL.append("AND subject = ANY(?) AND status = ANY(?) ");
			if (today) {
				SQL.append("AND received_on >= ? ");
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(EVENT_STATUSES)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY subject");

				query(tx, SQL.toString(), q -> q.addParameter(subjects).addParameter(EVENT_STATUSES)
					.executeAndFetch(handler));
			}
		});
	}

	private void taskTypeRefNameAverage(Map<String, AtomicLong> map, boolean today) {
		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String typeName = rs.getString("task_type").toLowerCase();
					String refName = rs.getString("task_refname").toLowerCase();
					long avg = rs.getLong("avg");

					// Init both counters right away if any today/non-today returned
					initMetric(map, String.format("%s.avg_task_execution_msec.%s_%s", PREFIX, typeName, refName));
					initMetric(map, String.format("%s.avg_task_execution_msec_today.%s_%s", PREFIX, typeName, refName));

					// Se the correct one
					String metricName = String.format("%s.avg_task_execution_msec%s.%s_%s", PREFIX, toLabel(today), typeName, refName);
					map.get(metricName).set(avg);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT task_type, task_refname, avg(extract('epoch' from end_time) - extract('epoch' from start_time)) as avg ");
			SQL.append("FROM task WHERE start_time IS NOT NULL AND end_time IS NOT NULL ");
			SQL.append("AND task_type = ANY(?) AND task_status = 'COMPLETED' ");
			if (today) {
				SQL.append("AND start_time >= ? ");
				SQL.append("GROUP BY task_type, task_refname");

				query(tx, SQL.toString(), q -> q.addParameter(TASK_TYPES)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY task_type, task_refname");

				query(tx, SQL.toString(), q -> q.addParameter(TASK_TYPES)
					.executeAndFetch(handler));
			}
		});
	}

	private void taskTypeRefNameCounters(Map<String, AtomicLong> map, boolean today) {
		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String typeName = rs.getString("task_type").toLowerCase();
					String refName = rs.getString("task_refname").toLowerCase();
					String status = rs.getString("task_status").toLowerCase();
					long count = rs.getLong("count");

					// Init counters. Total per typeName/refName + today/non-today
					initMetric(map, String.format("%s.task_%s_%s", PREFIX, typeName, refName));
					initMetric(map, String.format("%s.task_%s_%s_today", PREFIX, typeName, refName));

					// Init counters. Per typeName/refName/status + today/non-today
					for (String statusName : TASK_STATUSES) {
						initMetric(map, String.format("%s.task_%s_%s_%s", PREFIX, typeName, refName, statusName.toLowerCase()));
						initMetric(map, String.format("%s.task_%s_%s_%s_today", PREFIX, typeName, refName, statusName.toLowerCase()));
					}

					// Parent typeName + refName
					String metricName = String.format("%s.task_%s_%s%s", PREFIX, typeName, refName, toLabel(today));
					map.get(metricName).addAndGet(count);

					// typeName + refName + status
					metricName = String.format("%s.task_%s_%s_%s%s", PREFIX, typeName, refName, status, toLabel(today));
					map.get(metricName).addAndGet(count);
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT task_type, task_refname, task_status, count(*) as count ");
			SQL.append("FROM task WHERE task_type = ANY(?) AND task_status = ANY(?) ");
			if (today) {
				SQL.append("AND start_time >= ? ");
				SQL.append("GROUP BY task_type, task_refname, task_status");

				query(tx, SQL.toString(), q -> q.addParameter(TASK_TYPES).addParameter(TASK_STATUSES)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY task_type, task_refname, task_status");

				query(tx, SQL.toString(), q -> q.addParameter(TASK_TYPES).addParameter(TASK_STATUSES)
					.executeAndFetch(handler));
			}
		});
	}

	private void workflowAverage(Map<String, AtomicLong> map, boolean today, String shortName, Set<String> filtered) {
		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					long avg = rs.getLong("avg");

					String metricName = String.format("%s.avg_workflow_execution_sec%s.%s", PREFIX, toLabel(today), shortName);                    // Se the correct one
					map.put(metricName, new AtomicLong(avg / 1000));
				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT avg(extract('epoch' from end_time) - extract('epoch' from start_time)) as avg ");
			SQL.append("FROM workflow WHERE start_time IS NOT NULL AND end_time IS NOT NULL ");
			SQL.append("AND workflow_type = ANY(?) AND workflow_status = 'COMPLETED' ");
			if (today) {
				SQL.append("AND start_time >= ? ");

				query(tx, SQL.toString(), q -> q.addParameter(filtered)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				query(tx, SQL.toString(), q -> q.addParameter(filtered)
					.executeAndFetch(handler));
			}
		});
	}

	private void workflowCounters(Map<String, AtomicLong> map, boolean today, String shortName, Set<String> filtered) {
		List<String> workflowStatuses = today ? WORKFLOW_TODAY_STATUSES : WORKFLOW_OVERALL_STATUSES;

		// Init counters
		initMetric(map, String.format("%s.workflow_started%s", PREFIX, toLabel(today)));

		// Counter name per status
		for (String status : workflowStatuses) {
			initMetric(map, String.format("%s.workflow_%s%s", PREFIX, status.toLowerCase(), toLabel(today)));
		}

		// Counter name per workflow type and status
		initMetric(map, String.format("%s.workflow_started%s.%s", PREFIX, toLabel(today), shortName));
		for (String status : workflowStatuses) {
			String metricName = String.format("%s.workflow_%s%s.%s", PREFIX, status.toLowerCase(), toLabel(today), shortName);
			initMetric(map, metricName);
		}

		withTransaction(tx -> {
			ResultSetHandler<Object> handler = rs -> {
				while (rs.next()) {
					String typeName = rs.getString("workflow_type").toLowerCase();
					String statusName = rs.getString("workflow_status").toLowerCase();
					long count = rs.getLong("count");

					// Total started
					String metricName = String.format("%s.workflow_started%s", PREFIX, toLabel(today));
					map.get(metricName).addAndGet(count);

					// Started per workflow type
					metricName = String.format("%s.workflow_started%s.%s", PREFIX, toLabel(today), shortName);
					map.get(metricName).addAndGet(count);

					// Counter name per status
					metricName = String.format("%s.workflow_%s%s", PREFIX, statusName, toLabel(today));
					map.get(metricName).addAndGet(count);

					// Counter name per workflow type and status
					metricName = String.format("%s.workflow_%s%s.%s", PREFIX, statusName, toLabel(today), shortName);
					map.get(metricName).addAndGet(count);

				}
				return null;
			};

			StringBuilder SQL = new StringBuilder("SELECT workflow_type, workflow_status, count(*) as count ");
			SQL.append("FROM workflow WHERE start_time IS NOT NULL AND end_time IS NOT NULL ");
			SQL.append("AND workflow_type = ANY(?) AND workflow_status = ANY(?) ");
			if (today) {
				SQL.append("AND start_time >= ? ");
				SQL.append("GROUP BY workflow_type, workflow_status");

				query(tx, SQL.toString(), q -> q.addParameter(filtered).addParameter(workflowStatuses)
					.addTimestampParameter(getStartTime())
					.executeAndFetch(handler));
			} else {
				SQL.append("GROUP BY workflow_type, workflow_status");

				query(tx, SQL.toString(), q -> q.addParameter(filtered).addParameter(workflowStatuses)
					.executeAndFetch(handler));
			}
		});
	}
}

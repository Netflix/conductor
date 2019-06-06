package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.MetricsDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class AuroraMetricsDAO extends AuroraBaseDAO implements MetricsDAO {
	private static final Logger logger = LoggerFactory.getLogger(AuroraMetricsDAO.class);
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
	private MetadataDAO metadataDAO;

	@Inject
	public AuroraMetricsDAO(DataSource dataSource, ObjectMapper mapper, MetadataDAO metadataDAO, Configuration config) {
		super(dataSource, mapper);
		this.metadataDAO = metadataDAO;
	}

	@Override
	public Map<String, Object> getMetrics() {
		return null;
	}

	@Override
	public Map<String, Object> getAdminCounters() {
		Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
		adminCounters(metrics);
		return new HashMap<>(metrics);
	}

	@Override
	public Map<String, Object> getEventReceived() {
		return null;
	}

	@Override
	public Map<String, Object> getEventPublished() {
		return null;
	}

	@Override
	public Map<String, Object> getEventExecAverage() {
		return null;
	}

	@Override
	public Map<String, Object> getEventWaitAverage() {
		return null;
	}

	@Override
	public Map<String, Object> getTaskCounters() {
		return null;
	}

	@Override
	public Map<String, Object> getTaskAverage() {
		return null;
	}

	@Override
	public Map<String, Object> getWorkflowCounters() {
		return null;
	}

	@Override
	public Map<String, Object> getWorkflowAverage() {
		return null;
	}

	private void adminCounters(Map<String, AtomicLong> map) {
		Set<EventHandler> handlers = getHandlers();
		initMetric(map, String.format("%s.admin.active_handlers", PREFIX)).set(handlers.size());

		String SQL = "select count(*) from queue_message where queue_name = ?";
		long count = queryWithTransaction(SQL, q -> q.addParameter("_deciderqueue").executeScalar(Long.class));

		initMetric(map, String.format("%s.admin.decider_queue", PREFIX)).set(count);
	}

	private Set<EventHandler> getHandlers() {
		return metadataDAO.getEventHandlers().stream()
			.filter(EventHandler::isActive).collect(Collectors.toSet());
	}

	// Extracts subject name from each active handler
	private Set<String> getSubjects() {
		return getHandlers().stream()
			.map(eh -> eh.getEvent().split(":")[1]) // 0 - event bus, 1 - subject, 2 - queue
			.collect(Collectors.toSet());
	}

	private AtomicLong initMetric(Map<String, AtomicLong> map, String metricName) {
		return map.computeIfAbsent(metricName, s -> new AtomicLong(0));
	}

	private String toLabel(boolean today) {
		return today ? "_today" : "";
	}

}

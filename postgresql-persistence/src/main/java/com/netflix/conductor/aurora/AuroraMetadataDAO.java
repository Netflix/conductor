package com.netflix.conductor.aurora;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.MetadataDAO;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AuroraMetadataDAO extends AuroraBaseDAO implements MetadataDAO {
	private static final String PROP_TASKDEF_CACHE_REFRESH = "conductor.taskdef.cache.refresh.time.seconds";
	private static final int DEFAULT_TASKDEF_CACHE_REFRESH_SECONDS = 60;
	private final ConcurrentHashMap<String, TaskDef> taskDefCache = new ConcurrentHashMap<>();

	@Inject
	public AuroraMetadataDAO(DataSource dataSource, ObjectMapper mapper, Configuration config) {
		super(dataSource, mapper);
		refreshTaskDefs();

		int cacheRefreshTime = config.getIntProperty(PROP_TASKDEF_CACHE_REFRESH, DEFAULT_TASKDEF_CACHE_REFRESH_SECONDS);
		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleWithFixedDelay(this::refreshTaskDefs, cacheRefreshTime, cacheRefreshTime, TimeUnit.SECONDS);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				logger.info("Closing refreshTaskDefs pool");
				executorService.shutdown();
				executorService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				logger.debug("Closing refreshTaskDefs pool failed " + e.getMessage(), e);;
			}
		}));
	}

	@Override
	public String createTaskDef(TaskDef taskDef) {
		validate(taskDef);
		if (null == taskDef.getCreateTime() || taskDef.getCreateTime() < 1) {
			taskDef.setCreateTime(System.currentTimeMillis());
		}

		return insertOrUpdateTaskDef(taskDef);
	}

	@Override
	public String updateTaskDef(TaskDef taskDef) {
		validate(taskDef);
		taskDef.setUpdateTime(System.currentTimeMillis());
		return insertOrUpdateTaskDef(taskDef);
	}

	@Override
	public TaskDef getTaskDef(String name) {
		Preconditions.checkNotNull(name, "TaskDef name cannot be null");
		TaskDef taskDef = taskDefCache.get(name);
		if (taskDef == null) {
			if (logger.isTraceEnabled()) {
				logger.trace("Cache miss: {}", name);
			}
			taskDef = getTaskDefFromDB(name);
		}

		return taskDef;
	}

	@Override
	public List<TaskDef> getAllTaskDefs() {
		return getWithTransaction(this::findAllTaskDefs);
	}

	@Override
	public void removeTaskDef(String name) {
		final String SQL = "DELETE FROM meta_task_def WHERE name = ?";

		executeWithTransaction(SQL, q -> {
			if (!q.addParameter(name).executeDelete()) {
				throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "No such task definition");
			}

			taskDefCache.remove(name);
		});
	}

	@Override
	public void create(WorkflowDef def) {
		validate(def);
		if (null == def.getCreateTime() || def.getCreateTime() == 0) {
			def.setCreateTime(System.currentTimeMillis());
		}

		withTransaction(tx -> {
			if (workflowExists(tx, def)) {
				throw new ApplicationException(ApplicationException.Code.CONFLICT,
					"Workflow with " + def.key() + " already exists!");
			}

			insertOrUpdateWorkflowDef(tx, def);
		});
	}

	@Override
	public void update(WorkflowDef def) {
		validate(def);
		def.setUpdateTime(System.currentTimeMillis());
		withTransaction(tx -> insertOrUpdateWorkflowDef(tx, def));
	}

	@Override
	public void removeWorkflow(WorkflowDef def) {
		validate(def);
		String name = def.getName();
		int version = def.getVersion();
		final String SQL = "DELETE from meta_workflow_def WHERE name = ? AND version = ?";

		executeWithTransaction(SQL, q -> {
			if (!q.addParameter(name).addParameter(version).executeDelete()) {
				throw new ApplicationException(ApplicationException.Code.NOT_FOUND,
					String.format("No such workflow definition: %s version: %d", name, version));
			}
		});
	}

	@Override
	public WorkflowDef getLatest(String name) {
		final String SQL = "SELECT json_data FROM meta_workflow_def WHERE NAME = ? AND " +
			"version = latest_version";

		return queryWithTransaction(SQL,
			q -> q.addParameter(name).executeAndFetchFirst(WorkflowDef.class));
	}

	@Override
	public WorkflowDef get(String name, int version) {
		final String SQL = "SELECT json_data FROM meta_workflow_def WHERE NAME = ? AND version = ?";
		return queryWithTransaction(SQL, q -> q.addParameter(name)
			.addParameter(version)
			.executeAndFetchFirst(WorkflowDef.class));
	}

	@Override
	public List<String> findAll() {
		final String SQL = "SELECT DISTINCT name FROM meta_workflow_def";
		return queryWithTransaction(SQL, q -> q.executeAndFetch(String.class));
	}

	@Override
	public List<WorkflowDef> getAll() {
		final String SQL = "SELECT json_data FROM meta_workflow_def ORDER BY name, version";
		return queryWithTransaction(SQL, q -> q.executeAndFetch(WorkflowDef.class));
	}

	@Override
	public List<WorkflowDef> getAllLatest() {
		final String SQL = "SELECT json_data FROM meta_workflow_def WHERE version = " +
			"latest_version";
		return queryWithTransaction(SQL, q -> q.executeAndFetch(WorkflowDef.class));
	}

	@Override
	public List<WorkflowDef> getAllVersions(String name) {
		final String SQL = "SELECT json_data FROM meta_workflow_def WHERE name = ? " +
			"ORDER BY version";

		return queryWithTransaction(SQL,
			q -> q.addParameter(name).executeAndFetch(WorkflowDef.class));
	}

	@Override
	public void addEventHandler(EventHandler eventHandler) {
		Preconditions.checkNotNull(eventHandler.getName(), "EventHandler name cannot be null");

		final String SQL = "INSERT INTO meta_event_handler (name, event, active, json_data) " +
			"VALUES (?, ?, ?, ?)";

		withTransaction(tx -> {
			if (getEventHandler(tx, eventHandler.getName()) != null) {
				throw new ApplicationException(ApplicationException.Code.CONFLICT,
					"EventHandler with name " + eventHandler.getName() + " already exists!");
			}

			execute(tx, SQL, q -> q.addParameter(eventHandler.getName())
				.addParameter(eventHandler.getEvent())
				.addParameter(eventHandler.isActive())
				.addJsonParameter(eventHandler)
				.executeUpdate());
		});
	}

	@Override
	public void updateEventHandler(EventHandler eventHandler) {
		Preconditions.checkNotNull(eventHandler.getName(), "EventHandler name cannot be null");

		//@formatter:off
		final String SQL = "UPDATE meta_event_handler SET " +
			"event = ?, active = ?, json_data = ?, " +
			"modified_on = now() WHERE name = ?";
		//@formatter:on

		withTransaction(tx -> {
			EventHandler existing = getEventHandler(tx, eventHandler.getName());
			if (existing == null) {
				throw new ApplicationException(ApplicationException.Code.NOT_FOUND,
					"EventHandler with name " + eventHandler.getName() + " not found!");
			}

			execute(tx, SQL, q -> q.addParameter(eventHandler.getEvent())
				.addParameter(eventHandler.isActive())
				.addJsonParameter(eventHandler)
				.addParameter(eventHandler.getName())
				.executeUpdate());
		});
	}

	@Override
	public void removeEventHandlerStatus(String name) {
		final String SQL = "DELETE FROM meta_event_handler WHERE name = ?";

		withTransaction(tx -> {
			EventHandler existing = getEventHandler(tx, name);
			if (existing == null) {
				throw new ApplicationException(ApplicationException.Code.NOT_FOUND,
					"EventHandler with name " + name + " not found!");
			}

			execute(tx, SQL, q -> q.addParameter(name).executeDelete());
		});
	}

	@Override
	public List<EventHandler> getEventHandlers() {
		final String SQL = "SELECT json_data FROM meta_event_handler";
		return queryWithTransaction(SQL, q -> q.executeAndFetch(EventHandler.class));
	}

	@Override
	public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
		final String SQL = "SELECT json_data FROM meta_event_handler WHERE event = ?";
		return queryWithTransaction(SQL, q -> {
			q.addParameter(event);
			return q.executeAndFetch(rs -> {
				List<EventHandler> handlers = new ArrayList<>();
				while (rs.next()) {
					EventHandler h = readValue(rs.getString(1), EventHandler.class);
					if (!activeOnly || h.isActive()) {
						handlers.add(h);
					}
				}

				return handlers;
			});
		});
	}

	@Override
	public List<Pair<String, String>> getConfigs() {
		final String SQL = "SELECT name, value FROM meta_config";
		return queryWithTransaction(SQL, q -> q.executeAndFetch(rs -> {
			List<Pair<String, String>> configs = new ArrayList<>();
			while (rs.next()) {
				Pair<String, String> entry = Pair.of(rs.getString(1), rs.getString(2));
				configs.add(entry);
			}

			return configs;
		}));
	}

	@Override
	public void addConfig(String name, String value) {
		String SQL = "INSERT INTO meta_config (name, value) VALUES (?, ?) " +
			"ON CONFLICT ON CONSTRAINT meta_config_pkey DO UPDATE SET value = ? ";
		executeWithTransaction(SQL, q -> q
			.addParameter(name)
			.addParameter(value)
			.addParameter(value)
			.executeUpdate());
	}

	@Override
	public void updateConfig(String name, String value) {
		String SQL = "UPDATE meta_config SET value = ? WHERE upper(name) = ?";
		executeWithTransaction(SQL, q -> q
			.addParameter(value)
			.addParameter(name.toUpperCase())
			.executeUpdate());
	}

	@Override
	public void deleteConfig(String name) {
		String SQL = "DELETE FROM meta_config WHERE upper(name) = ?";
		executeWithTransaction(SQL, q -> q
			.addParameter(name.toUpperCase())
			.executeDelete());
	}

	private void validate(TaskDef taskDef) {
		Preconditions.checkNotNull(taskDef, "TaskDef object cannot be null");
		Preconditions.checkNotNull(taskDef.getName(), "TaskDef name cannot be null");
	}

	private void validate(WorkflowDef def) {
		Preconditions.checkNotNull(def, "WorkflowDef object cannot be null");
		Preconditions.checkNotNull(def.getName(), "WorkflowDef name cannot be null");
	}

	private String insertOrUpdateTaskDef(TaskDef taskDef) {
		final String UPDATE_SQL = "UPDATE meta_task_def SET json_data = ?, modified_on = now() WHERE name = ?";

		final String INSERT_SQL = "INSERT INTO meta_task_def (name, json_data) VALUES (?, ?)";

		return getWithTransaction(tx -> {
			execute(tx, UPDATE_SQL, update -> {
				int result = update.addJsonParameter(taskDef).addParameter(taskDef.getName()).executeUpdate();
				if (result == 0) {
					execute(tx, INSERT_SQL,
						insert -> insert.addParameter(taskDef.getName()).addJsonParameter(taskDef).executeUpdate());
				}
			});

			taskDefCache.put(taskDef.getName(), taskDef);
			return taskDef.getName();
		});
	}


	private List<TaskDef> findAllTaskDefs(Connection tx) {
		final String SQL = "SELECT json_data FROM meta_task_def";

		return query(tx, SQL, q -> q.executeAndFetch(TaskDef.class));
	}

	private void refreshTaskDefs() {
		try {
			List<TaskDef> taskDefs = getAllTaskDefs();

			Map<String, TaskDef> taskDefMap = taskDefs.stream()
				.collect(Collectors.toMap(TaskDef::getName, taskDef -> taskDef));

			synchronized (taskDefCache) {
				taskDefCache.clear();
				taskDefCache.putAll(taskDefMap);
			}

			if (logger.isTraceEnabled()) {
				logger.trace("Refreshed {} TaskDefs", taskDefCache.size());
			}
		} catch (Exception e) {
			logger.error("refresh TaskDefs failed ", e);
		}
	}

	private TaskDef getTaskDefFromDB(String name) {
		final String SQL = "SELECT json_data FROM meta_task_def WHERE name = ?";

		return queryWithTransaction(SQL,
			q -> q.addParameter(name).executeAndFetchFirst(TaskDef.class));
	}

	private Boolean workflowExists(Connection connection, WorkflowDef def) {
		final String SQL = "SELECT COUNT(*) FROM meta_workflow_def WHERE name = ? AND version = ?";

		return query(connection, SQL,
			q -> q.addParameter(def.getName()).addParameter(def.getVersion()).exists());
	}

	private void insertOrUpdateWorkflowDef(Connection tx, WorkflowDef def) {
		Optional<Integer> version = getLatestVersion(tx, def);
		if (!version.isPresent() || version.get() < def.getVersion()) {
			final String SQL = "INSERT INTO meta_workflow_def (name, version, json_data) VALUES (?, ?, ?)";

			execute(tx, SQL, q -> q.addParameter(def.getName())
				.addParameter(def.getVersion())
				.addJsonParameter(def)
				.executeUpdate());
		} else {
			final String SQL = "UPDATE meta_workflow_def " +
					"SET json_data = ?, modified_on = now() " +
					"WHERE name = ? AND version = ?";

			execute(tx, SQL, q -> q.addJsonParameter(def)
				.addParameter(def.getName())
				.addParameter(def.getVersion())
				.executeUpdate());
		}

		updateLatestVersion(tx, def);
	}

	private Optional<Integer> getLatestVersion(Connection tx, WorkflowDef def) {
		final String SQL = "SELECT max(version) AS version FROM meta_workflow_def WHERE name = ?";

		Integer val = query(tx, SQL, q -> q.addParameter(def.getName()).executeScalar(Integer.class));

		return Optional.ofNullable(val);
	}

	private void updateLatestVersion(Connection tx, WorkflowDef def) {
		final String SQL = "UPDATE meta_workflow_def SET latest_version = ? WHERE name = ?";

		execute(tx, SQL,
			q -> q.addParameter(def.getVersion()).addParameter(def.getName()).executeUpdate());
	}

	private EventHandler getEventHandler(Connection connection, String name) {
		final String SQL = "SELECT json_data FROM meta_event_handler WHERE name = ?";

		return query(connection, SQL,
			q -> q.addParameter(name).executeAndFetchFirst(EventHandler.class));
	}
}

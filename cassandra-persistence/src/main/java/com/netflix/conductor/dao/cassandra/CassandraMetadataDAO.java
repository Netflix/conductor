/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.dao.cassandra;

import static com.netflix.conductor.util.Constants.TASK_DEFINITION_KEY;
import static com.netflix.conductor.util.Constants.TASK_DEFS_KEY;
import static com.netflix.conductor.util.Constants.WORKFLOW_DEFINITION_KEY;
import static com.netflix.conductor.util.Constants.WORKFLOW_DEF_INDEX_KEY;
import static com.netflix.conductor.util.Constants.WORKFLOW_DEF_NAME_VERSION_KEY;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.cassandra.CassandraConfiguration;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.util.Statements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraMetadataDAO extends CassandraBaseDAO implements MetadataDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataDAO.class);
    private static final String CLASS_NAME = CassandraMetadataDAO.class.getSimpleName();
    private static final String INDEX_DELIMITER = "/";

    private Map<String, TaskDef> taskDefCache = new HashMap<>();

    private final PreparedStatement insertWorkflowDefStatement;
    private final PreparedStatement insertWorkflowDefVersionIndexStatement;
    private final PreparedStatement insertTaskDefStatement;

    private final PreparedStatement selectWorkflowDefStatement;
    private final PreparedStatement selectAllWorkflowDefVersionsByNameStatement;
    private final PreparedStatement selectAllWorkflowDefsStatement;
    private final PreparedStatement selectTaskDefStatement;
    private final PreparedStatement selectAllTaskDefsStatement;

    private final PreparedStatement deleteWorkflowDefStatement;
    private final PreparedStatement deleteWorkflowDefIndexStatement;
    private final PreparedStatement deleteTaskDefStatement;

    @Inject
    public CassandraMetadataDAO(Session session, ObjectMapper objectMapper, CassandraConfiguration config,
        Statements statements) {
        super(session, objectMapper, config);

        this.insertWorkflowDefStatement = session.prepare(statements.getInsertWorkflowDefStatement())
            .setConsistencyLevel(config.getWriteConsistencyLevel());
        this.insertWorkflowDefVersionIndexStatement = session
            .prepare(statements.getInsertWorkflowDefVersionIndexStatement())
            .setConsistencyLevel(config.getWriteConsistencyLevel());
        this.insertTaskDefStatement = session.prepare(statements.getInsertTaskDefStatement())
            .setConsistencyLevel(config.getWriteConsistencyLevel());

        this.selectWorkflowDefStatement = session.prepare(statements.getSelectWorkflowDefStatement())
            .setConsistencyLevel(config.getReadConsistencyLevel());
        this.selectAllWorkflowDefVersionsByNameStatement = session
            .prepare(statements.getSelectAllWorkflowDefVersionsByNameStatement())
            .setConsistencyLevel(config.getReadConsistencyLevel());
        this.selectAllWorkflowDefsStatement = session.prepare(statements.getSelectAllWorkflowDefsStatement())
            .setConsistencyLevel(config.getReadConsistencyLevel());
        this.selectTaskDefStatement = session.prepare(statements.getSelectTaskDefStatement())
            .setConsistencyLevel(config.getReadConsistencyLevel());
        this.selectAllTaskDefsStatement = session.prepare(statements.getSelectAllTaskDefsStatement())
            .setConsistencyLevel(config.getReadConsistencyLevel());

        this.deleteWorkflowDefStatement = session.prepare(statements.getDeleteWorkflowDefStatement())
            .setConsistencyLevel(config.getWriteConsistencyLevel());
        this.deleteWorkflowDefIndexStatement = session.prepare(statements.getDeleteWorkflowDefIndexStatement())
            .setConsistencyLevel(config.getWriteConsistencyLevel());
        this.deleteTaskDefStatement = session.prepare(statements.getDeleteTaskDefStatement())
            .setConsistencyLevel(config.getWriteConsistencyLevel());

        int cacheRefreshTime = config.getTaskDefRefreshTimeSecsDefaultValue();
        Executors.newSingleThreadScheduledExecutor()
            .scheduleWithFixedDelay(this::refreshTaskDefsCache, 0, cacheRefreshTime, TimeUnit.SECONDS);
    }

    @Override
    public void createTaskDef(TaskDef taskDef) {
        taskDef.setCreateTime(System.currentTimeMillis());
        insertOrUpdateTaskDef(taskDef);
    }

    @Override
    public String updateTaskDef(TaskDef taskDef) {
        taskDef.setUpdateTime(System.currentTimeMillis());
        return insertOrUpdateTaskDef(taskDef);
    }

    @Override
    public TaskDef getTaskDef(String name) {
        return Optional.ofNullable(taskDefCache.get(name))
            .orElseGet(() -> getTaskDefFromDB(name));
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        if (taskDefCache.size() == 0) {
            refreshTaskDefsCache();
        }
        return new ArrayList<>(taskDefCache.values());
    }

    @Override
    public void removeTaskDef(String name) {
        try {
            recordCassandraDaoRequests("removeTaskDef");
            session.execute(deleteTaskDefStatement.bind(name));
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "removeTaskDef");
            String errorMsg = String.format("Failed to remove task definition: %s", name);
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
        refreshTaskDefsCache();
    }

    @Override
    public void createWorkflowDef(WorkflowDef workflowDef) {
        if (workflowDefExists(workflowDef)) {
            throw new ApplicationException(Code.CONFLICT, String.format("Workflow: %s, version: %s already exists!",
                workflowDef.getName(), workflowDef.getVersion()));
        }
        workflowDef.setCreateTime(System.currentTimeMillis());
        insertOrUpdateWorkflowDef(workflowDef);
    }

    @Override
    public void updateWorkflowDef(WorkflowDef workflowDef) {
        workflowDef.setUpdateTime(System.currentTimeMillis());
        insertOrUpdateWorkflowDef(workflowDef);
    }

    @Override
    public Optional<WorkflowDef> getLatestWorkflowDef(String name) {
        List<WorkflowDef> workflowDefList = getAllWorkflowDefVersions(name);
        if (workflowDefList != null && workflowDefList.size() > 0) {
            workflowDefList.sort(Comparator.comparingInt(WorkflowDef::getVersion));
            return Optional.of(workflowDefList.get(workflowDefList.size() - 1));
        }
        return Optional.empty();
    }

    @Override
    public Optional<WorkflowDef> getWorkflowDef(String name, int version) {
        try {
            recordCassandraDaoRequests("getWorkflowDef");
            ResultSet resultSet = session.execute(selectWorkflowDefStatement.bind(name, version));
            WorkflowDef workflowDef = Optional.ofNullable(resultSet.one())
                .map(row -> readValue(row.getString(WORKFLOW_DEFINITION_KEY), WorkflowDef.class))
                .orElse(null);
            return Optional.ofNullable(workflowDef);
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "getTaskDef");
            String errorMsg = String.format("Error fetching workflow def: %s/%d", name, version);
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    @Override
    public void removeWorkflowDef(String name, Integer version) {
        try {
            session.execute(deleteWorkflowDefStatement.bind(name, version));
            session.execute(deleteWorkflowDefIndexStatement.bind(WORKFLOW_DEF_INDEX_KEY,
                getWorkflowDefIndexValue(name, version)));
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "removeWorkflowDef");
            String errorMsg = String.format("Failed to remove workflow definition: %s/%d", name, version);
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WorkflowDef> getAllWorkflowDefs() {
        try {
            ResultSet resultSet = session.execute(selectAllWorkflowDefsStatement.bind(WORKFLOW_DEF_INDEX_KEY));
            List<Row> rows = resultSet.all();
            if (rows.size() == 0) {
                LOGGER.info("No workflow definitions were found.");
                return Collections.EMPTY_LIST;
            }
            return rows.stream()
                .map(row -> {
                    String defNameVersion = row.getString(WORKFLOW_DEF_NAME_VERSION_KEY);
                    String[] tokens = defNameVersion.split(INDEX_DELIMITER);
                    return getWorkflowDef(tokens[0], Integer.parseInt(tokens[1])).orElse(null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "getAllWorkflowDefs");
            String errorMsg = "Error retrieving all workflow defs";
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    private void refreshTaskDefsCache() {
        try {
            Map<String, TaskDef> map = new HashMap<>();
            getAllTaskDefsFromDB().forEach(taskDef -> map.put(taskDef.getName(), taskDef));
            this.taskDefCache = map;
            LOGGER.debug("Refreshed task defs, total num: " + this.taskDefCache.size());
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "refreshTaskDefs");
            LOGGER.error("refresh TaskDefs failed ", e);
        }
    }

    private TaskDef getTaskDefFromDB(String name) {
        try {
            ResultSet resultSet = session.execute(selectTaskDefStatement.bind(name));
            recordCassandraDaoRequests("getTaskDef");
            return Optional.ofNullable(resultSet.one())
                .map(row -> readValue(row.getString(TASK_DEFINITION_KEY), TaskDef.class))
                .orElse(null);
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "getTaskDef");
            String errorMsg = String.format("Failed to get task def: %s", name);
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<TaskDef> getAllTaskDefsFromDB() {
        try {
            ResultSet resultSet = session.execute(selectAllTaskDefsStatement.bind(TASK_DEFS_KEY));
            List<Row> rows = resultSet.all();
            if (rows.size() == 0) {
                LOGGER.info("No task definitions were found.");
                return Collections.EMPTY_LIST;
            }
            return rows.stream()
                .map(row -> readValue(row.getString(TASK_DEFINITION_KEY), TaskDef.class))
                .collect(Collectors.toList());
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "getAllTaskDefs");
            String errorMsg = "Failed to get all task defs";
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    private List<WorkflowDef> getAllWorkflowDefVersions(String name) {
        try {
            ResultSet resultSet = session.execute(selectAllWorkflowDefVersionsByNameStatement.bind(name));
            recordCassandraDaoRequests("getAllWorkflowDefVersions", "n/a", name);
            List<Row> rows = resultSet.all();
            if (rows.size() == 0) {
                LOGGER.info("Not workflow definitions were found for : {}", name);
                return null;
            }
            return rows.stream()
                .map(row -> readValue(row.getString(WORKFLOW_DEFINITION_KEY), WorkflowDef.class))
                .collect(Collectors.toList());
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "getAllWorkflowDefVersions");
            String errorMsg = String.format("Failed to get workflows defs for : %s", name);
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    private String insertOrUpdateTaskDef(TaskDef taskDef) {
        try {
            String taskDefinition = toJson(taskDef);
            session.execute(insertTaskDefStatement.bind(taskDef.getName(), taskDefinition));
            recordCassandraDaoRequests("storeTaskDef");
            recordCassandraDaoPayloadSize("storeTaskDef", taskDefinition.length(), taskDef.getName(), "n/a");
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "insertOrUpdateTaskDef");
            String errorMsg = String.format("Error creating/updating task definition: %s", taskDef.getName());
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
        refreshTaskDefsCache();
        return taskDef.getName();
    }

    @VisibleForTesting
    boolean workflowDefExists(WorkflowDef workflowDef) {
        return getWorkflowDef(workflowDef.getName(), workflowDef.getVersion()).isPresent();
    }

    String getWorkflowDefIndexValue(String name, int version) {
        return name + INDEX_DELIMITER + version;
    }

    private void insertOrUpdateWorkflowDef(WorkflowDef def) {
        try {
            String workflowDefinition = toJson(def);
            session.execute(insertWorkflowDefStatement.bind(def.getName(), def.getVersion(), workflowDefinition));
            String workflowDefIndex = getWorkflowDefIndexValue(def.getName(), def.getVersion());
            session.execute(insertWorkflowDefVersionIndexStatement.bind(workflowDefIndex, workflowDefIndex));
            recordCassandraDaoRequests("storeWorkflowDef");
            recordCassandraDaoPayloadSize("storeWorkflwoDef", workflowDefinition.length(), "n/a", def.getName());
        } catch (Exception e) {
            Monitors.error(CLASS_NAME, "insertOrUpdateWorkflowDef");
            String errorMsg = String.format("Error creating/updating workflow definition: %s/%d",
                def.getName(), def.getVersion());
            LOGGER.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }
}

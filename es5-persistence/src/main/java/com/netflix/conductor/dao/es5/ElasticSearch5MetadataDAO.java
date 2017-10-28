/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.dao.es5;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.MetadataDAO;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
public class ElasticSearch5MetadataDAO extends ElasticSearch5BaseDAO implements MetadataDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5MetadataDAO.class);
    private Map<String, TaskDef> taskDefCache = new HashMap<>();

    // Keys Families
    private final static String TASK_DEFS = "TASK_DEFS";
    private final static String WORKFLOW_DEFS = "WORKFLOW_DEFS";
    private final static String EVENT_HANDLERS = "EVENT_HANDLERS";

    @Inject
    public ElasticSearch5MetadataDAO(Client client, Configuration config, ObjectMapper mapper) {
        super(client, config, mapper, "metadata");

        refreshTaskDefs();
        int cacheRefreshTime = config.getIntProperty("conductor.taskdef.cache.refresh.time.seconds", 60);
        Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(this::refreshTaskDefs, cacheRefreshTime, cacheRefreshTime, TimeUnit.SECONDS);
    }

    @Override
    public String createTaskDef(TaskDef taskDef) {
        logger.debug("createTaskDef: taskDef={}", toJson(taskDef));
        taskDef.setCreateTime(System.currentTimeMillis());
        return insertOrUpdate(taskDef);
    }

    @Override
    public String updateTaskDef(TaskDef taskDef) {
        logger.debug("updateTaskDef: taskDef={}", toJson(taskDef));
        taskDef.setUpdateTime(System.currentTimeMillis());
        return insertOrUpdate(taskDef);
    }

    @Override
    public TaskDef getTaskDef(String name) {
        logger.debug("getTaskDef: name={}", name);
        Preconditions.checkNotNull(name, "TaskDef name cannot be null");
        TaskDef taskDef = taskDefCache.get(name);
        if(taskDef != null) {
            logger.debug("getTaskDef: found in cache={}", toJson(taskDef));
            return taskDef;
        }

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        GetResponse response = client.prepareGet(indexName, typeName, name).get();
        if (!response.isExists()) {
            logger.debug("getTaskDef: no taskDef found");
            return null;
        }

        taskDef = convert(response.getSource(), TaskDef.class);
        logger.debug("getTaskDef: result={}", toJson(taskDef));

        return taskDef;
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        logger.debug("getAllTaskDefs");

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        return findAll(indexName, typeName, TaskDef.class);
    }

    @Override
    public void removeTaskDef(String name) {
        logger.debug("removeTaskDef: name={}", name);

        Preconditions.checkNotNull(name, "TaskDef name cannot be null");
        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        client.prepareDelete(indexName, typeName, name).get();

        logger.debug("removeTaskDef: done");
    }

    @Override
    public void create(WorkflowDef workflowDef) {
        logger.debug("create: workflowDef={}", toJson(workflowDef));
        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        String id = workflowDef.getName() + NAMESPACE_SEP + String.valueOf(workflowDef.getVersion());
        GetResponse record = client.prepareGet(indexName, typeName, id).get();
        if (record.isExists()) {
            throw new ApplicationException(ApplicationException.Code.CONFLICT, "Workflow with " + workflowDef.key() + " already exists!");
        }
        workflowDef.setCreateTime(System.currentTimeMillis());
        insertOrUpdate(workflowDef);
    }

    @Override
    public void update(WorkflowDef workflowDef) {
        logger.debug("update: workflowDef={}", toJson(workflowDef));
        workflowDef.setUpdateTime(System.currentTimeMillis());
        insertOrUpdate(workflowDef);
    }

    @Override
    public WorkflowDef getLatest(String name) {
        logger.debug("getLatest: name={}", name);
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        List<WorkflowDef> items = getAllVersions(name);
        logger.debug("getLatest: items={}", toJson(items));
        if (items.isEmpty()) {
            return null;
        }

        items.sort(Comparator.comparingInt(WorkflowDef::getVersion).reversed());
        WorkflowDef workflowDef = items.get(0);
        logger.debug("getLatest: result={}", toJson(workflowDef));

        return workflowDef;
    }

    @Override
    public WorkflowDef get(String name, int version) {
        logger.debug("get: name={}, version={}", name, version);
        return getByVersion(name, String.valueOf(version));
    }

    @Override
    public List<String> findAll() {
        logger.debug("findAll");
        List<WorkflowDef> items = getAll();
        logger.debug("findAll: items={}", toJson(items));
        if (items.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> result = items.stream().map(WorkflowDef::getName).collect(Collectors.toList());
        logger.debug("findAll: result={}", toJson(result));
        return result;
    }

    @Override
    public List<WorkflowDef> getAll() {
        logger.debug("getAll");
        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        return findAll(indexName, typeName, WorkflowDef.class);
    }

    @Override
    public List<WorkflowDef> getAllLatest() {
        logger.debug("getAllLatest");
        List<String> names = findAll();
        if (names.isEmpty()) {
            return Collections.emptyList();
        }

        List<WorkflowDef> result = names.stream().map(this::getLatest).collect(Collectors.toList());
        logger.debug("getAllLatest: result={}", toJson(result));
        return result;
    }

    @Override
    public List<WorkflowDef> getAllVersions(String name) {
        logger.debug("getAllVersions: name={}", name);
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        QueryBuilder query = QueryBuilders.matchQuery("name", name);
        List<WorkflowDef> result = findAll(indexName, typeName, query, WorkflowDef.class);

        logger.debug("getAllVersions: result={}", toJson(result));
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addEventHandler(EventHandler eventHandler) {
        logger.debug("addEventHandler: eventHandler={}", toJson(eventHandler));
        Preconditions.checkNotNull(eventHandler.getName(), "Missing Name");

        EventHandler existing = getEventHandler(eventHandler.getName());
        if(existing != null) {
            throw new ApplicationException(ApplicationException.Code.CONFLICT, "EventHandler with name " + eventHandler.getName() + " already exists!");
        }

        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);

        ensureIndexExists(indexName);

        client.prepareIndex(indexName, typeName, eventHandler.getName()).setSource(toMap(eventHandler)) .get();

        logger.debug("addEventHandler: done");
    }

    @Override
    public void updateEventHandler(EventHandler eventHandler) {
        logger.debug("updateEventHandler: eventHandler={}", toJson(eventHandler));
        Preconditions.checkNotNull(eventHandler.getName(), "Missing Name");

        EventHandler existing = getEventHandler(eventHandler.getName());
        if(existing == null) {
            throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "EventHandler with name " + eventHandler.getName() + " not found!");
        }

        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);

        ensureIndexExists(indexName);

        client.prepareUpdate(indexName, typeName, eventHandler.getName()).setDoc(toMap(eventHandler)).get();

        logger.debug("updateEventHandler: done");
    }

    @Override
    public void removeEventHandlerStatus(String name) {
        logger.debug("removeEventHandlerStatus: name={}", name);

        EventHandler existing = getEventHandler(name);
        if(existing == null) {
            throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "EventHandler with name " + name + " not found!");
        }

        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);

        ensureIndexExists(indexName);

        client.prepareDelete(indexName, typeName, name).get();

        logger.debug("removeEventHandlerStatus: done");
    }

    @Override
    public List<EventHandler> getEventHandlers() {
        logger.debug("getEventHandlers");
        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);

        ensureIndexExists(indexName);

        return findAll(indexName, typeName, EventHandler.class);
    }

    @Override
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
        logger.debug("getEventHandlersForEvent: event={}, activeOnly={}", event, activeOnly);

        List<EventHandler> handlers = getEventHandlers();
        logger.debug("getEventHandlersForEvent: found={}", toJson(handlers));
        if (handlers.isEmpty()) {
            return Collections.emptyList();
        }

        handlers = handlers.stream().filter(eh -> eh.getEvent().equals(event) && (!activeOnly || eh.isActive()))
                .collect(Collectors.toList());
        logger.debug("getEventHandlersForEvent: result={}", toJson(handlers));

        return handlers;
    }

    private WorkflowDef getByVersion(String name, String version) {
        logger.debug("getByVersion: name={}, version={}", name, version);
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        GetResponse record = client.prepareGet(indexName, typeName, name + NAMESPACE_SEP + version).get();
        if (!record.isExists()) {
            return null;
        }

        return convert(record.getSource(), WorkflowDef.class);
    }

    private EventHandler getEventHandler(String name) {
        logger.debug("getEventHandler: name={}", name);
        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);

        ensureIndexExists(indexName);

        GetResponse response = client.prepareGet(indexName, typeName, name).get();
        if (!response.isExists()) {
            return null;
        }
        EventHandler handler = convert(response.getSource(), EventHandler.class);;

        logger.debug("getEventHandler: result={}", toJson(handler));
        return handler;
    }

    private String insertOrUpdate(TaskDef def) {
        logger.debug("insertOrUpdate: taskDef={}", toJson(def));
        Preconditions.checkNotNull(def, "TaskDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "TaskDef name cannot be null");

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        client.prepareUpdate(indexName, typeName, def.getName())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setDocAsUpsert(true)
                .setDoc(toMap(def))
                .get();
        refreshTaskDefs();
        logger.debug("insertOrUpdate: done {}", def.getName());
        return def.getName();
    }

    private void insertOrUpdate(WorkflowDef def) {
        logger.debug("insertOrUpdate: workflowDef={}", toJson(def));
        Preconditions.checkNotNull(def, "WorkflowDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        String id = def.getName() + NAMESPACE_SEP + String.valueOf(def.getVersion());
        client.prepareUpdate(indexName, typeName, id)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setDocAsUpsert(true)
                .setDoc(toMap(def))
                .get();

        logger.debug("insertOrUpdate: done");
    }

    private void refreshTaskDefs() {
        logger.debug("refreshTaskDefs");

        Map<String, TaskDef> map = new HashMap<>();
        getAllTaskDefs().forEach(taskDef -> map.put(taskDef.getName(), taskDef));
        this.taskDefCache = map;

        logger.debug("refreshTaskDefs: task defs={}", map);
    }

}
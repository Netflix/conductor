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
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchQueryBuilder;
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
public class ElasticSearch5MetadataDAO implements MetadataDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5MetadataDAO.class);
    private Map<String, TaskDef> taskDefCache = new HashMap<>();
    private ObjectMapper mapper;
    private Client client;
    private String prefix;
    private String domain;
    private String stack;

    // Keys Families
    private final static String NAMESPACE_SEP = ".";
    private final static String TASK_DEFS = "TASK_DEFS";
    private final static String WORKFLOW_DEFS = "WORKFLOW_DEFS";
    private final static String EVENT_HANDLERS = "EVENT_HANDLERS";
    private final static String EVENT_HANDLERS_BY_EVENT = "EVENT_HANDLERS_BY_EVENT";

    @Inject
    public ElasticSearch5MetadataDAO(Client client, Configuration config, ObjectMapper mapper) {
        this.client = client;
        this.mapper = mapper;
        domain = config.getProperty("workflow.dyno.keyspace.domain", null);
        prefix = config.getProperty("workflow.namespace.prefix", null);
        stack = config.getStack();

        refreshTaskDefs();
        int cacheRefreshTime = config.getIntProperty("conductor.taskdef.cache.refresh.time.seconds", 60);
        Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(this::refreshTaskDefs, cacheRefreshTime, cacheRefreshTime, TimeUnit.SECONDS);
    }

    @Override
    public String createTaskDef(TaskDef taskDef) {
        taskDef.setCreateTime(System.currentTimeMillis());
        return insertOrUpdate(taskDef);
    }

    @Override
    public String updateTaskDef(TaskDef taskDef) {
        taskDef.setUpdateTime(System.currentTimeMillis());
        return insertOrUpdate(taskDef);
    }

    @Override
    public TaskDef getTaskDef(String name) {
        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        GetResponse response = client.prepareGet(indexName, typeName, name).get();
        return toObject(response.getSource(), TaskDef.class);
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        return findAll(indexName, typeName, TaskDef.class);
    }

    @Override
    public void removeTaskDef(String name) {
        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        client.prepareDelete(indexName, typeName, name).get();
    }

    @Override
    public void create(WorkflowDef workflowDef) {
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
        workflowDef.setUpdateTime(System.currentTimeMillis());
        insertOrUpdate(workflowDef);
    }

    private WorkflowDef getByVersion(String name, String version) {
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        GetResponse record = client.prepareGet(indexName, typeName, name + NAMESPACE_SEP + version).get();
        return toObject(record.getSource(), WorkflowDef.class);
    }

    @Override
    public WorkflowDef getLatest(String name) {
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        List<WorkflowDef> items = getAllVersions(name);
        if (items.isEmpty()) {
            return null;
        }

        items.sort(Comparator.comparingInt(WorkflowDef::getVersion).reversed());
        return items.get(0);
    }

    @Override
    public WorkflowDef get(String name, int version) {
        return getByVersion(name, String.valueOf(version));
    }

    @Override
    public List<String> findAll() {
        List<WorkflowDef> items = getAll();
        if (items.isEmpty()) {
            return Collections.emptyList();
        }

        return items.stream().map(WorkflowDef::getName).collect(Collectors.toList());
    }

    @Override
    public List<WorkflowDef> getAll() {
        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        return findAll(indexName, typeName, WorkflowDef.class);
    }

    @Override
    public List<WorkflowDef> getAllLatest() {
        List<String> names = findAll();
        if (names.isEmpty()) {
            return Collections.emptyList();
        }

        return names.stream().map(this::getLatest).collect(Collectors.toList());
    }

    @Override
    public List<WorkflowDef> getAllVersions(String name) {
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        ensureIndexExists(indexName);

        MatchQueryBuilder query = QueryBuilders.matchQuery("name", name);
        SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(0).get();
        int size = (int) response.getHits().getTotalHits();
        if (size == 0) {
            return Collections.emptyList();
        }

        response = client.prepareSearch(indexName).setTypes(typeName).setQuery(query).setSize(size).get();
        return Arrays.stream(response.getHits().getHits())
                .map(item -> toObject(item.getSource(), WorkflowDef.class))
                .collect(Collectors.toList());
    }

    @Override
    public void addEventHandler(EventHandler eventHandler) {
    }

    @Override
    public void updateEventHandler(EventHandler eventHandler) {
    }

    @Override
    public void removeEventHandlerStatus(String name) {
    }

    @Override
    public List<EventHandler> getEventHandlers() {
        return Collections.emptyList();
    }

    @Override
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
        return Collections.emptyList();
    }

    private <T> List<T> findAll(String indexName, String typeName, Class<T> clazz) {
        SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setSize(0).get();
        int size = (int) response.getHits().getTotalHits();
        if (size == 0) {
            return Collections.emptyList();
        }

        response = client.prepareSearch(indexName).setTypes(typeName).setSize(size).get();
        return Arrays.stream(response.getHits().getHits())
                .map(hit -> toObject(hit.getSource(), clazz))
                .collect(Collectors.toList());
    }

    private String insertOrUpdate(TaskDef def) {
        Preconditions.checkNotNull(def, "TaskDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "TaskDef name cannot be null");

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        ensureIndexExists(indexName);

        client.prepareUpdate(indexName, typeName, def.getName()).setDocAsUpsert(true).setDoc(toMap(def)).get();
        refreshTaskDefs();
        return def.getName();
    }

    private void insertOrUpdate(WorkflowDef def) {
        Preconditions.checkNotNull(def, "WorkflowDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        String id = def.getName() + NAMESPACE_SEP + String.valueOf(def.getVersion());
        client.prepareUpdate(indexName, typeName, id).setDocAsUpsert(true).setDoc(toMap(def)).get();
//        // Let's re-update the 'latest' based on the version list
//        List<Integer> versions = getAllVersions(def.getName()).stream()
//                .map(WorkflowDef::getVersion).collect(Collectors.toList());
//        Integer maxVer = Collections.max(versions);
//        WorkflowDef maxVerDef = get(def.getName(), maxVer);
//        String latestKey = def.getName() + NAMESPACE_SEP + LATEST;
//        client.prepareUpdate(indexName, typeName, latestKey).setDocAsUpsert(true).setDoc(toMap(maxVerDef)).get();
    }

    private void refreshTaskDefs() {
        Map<String, TaskDef> map = new HashMap<>();
        getAllTaskDefs().forEach(taskDef -> map.put(taskDef.getName(), taskDef));
        this.taskDefCache = map;
        logger.debug("Refreshed task defs " + this.taskDefCache.size());
    }

    private String toIndexName(String... nsValues) {
        StringBuilder namespaceKey = new StringBuilder(prefix).append(NAMESPACE_SEP).append("metadata").append(NAMESPACE_SEP);
        if (StringUtils.isNotEmpty(stack)) {
            namespaceKey.append(stack).append(NAMESPACE_SEP);
        }
        if (StringUtils.isNotEmpty(domain)) {
            namespaceKey.append(domain).append(NAMESPACE_SEP);
        }
        for (int i = 0; i < nsValues.length; i++) {
            namespaceKey.append(nsValues[i]);
            if (i < nsValues.length - 1) {
                namespaceKey.append(NAMESPACE_SEP);
            }
        }
        return namespaceKey.toString().toLowerCase();
    }

    private void ensureIndexExists(String indexName) {
        try {
            client.admin().indices().prepareGetIndex().addIndices(indexName).get();
        } catch (IndexNotFoundException notFound) {
            try {
                client.admin().indices().prepareCreate(indexName).get();
            } catch (ResourceAlreadyExistsException ignore) {
            } catch (Exception ex) {
                logger.error("ensureIndexExists: Failed for " + indexName + " with " + ex.getMessage(), ex);
            }
        }
    }

    private String toTypeName(String typeName) {
        return typeName.replace("_", "");
    }

    private Map toMap(Object value) {
        return mapper.convertValue(value, HashMap.class);
    }

    private <T> T toObject(Map json, Class<T> clazz) {
        return mapper.convertValue(json, clazz);
    }
}
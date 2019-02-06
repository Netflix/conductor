package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.MetadataDAO;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
public class Elasticsearch6RestMetadataDAO extends Elasticsearch6RestAbstractDAO implements MetadataDAO {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch6RestMetadataDAO.class);
    // Keys Families
    private final static String CONFIG = "CONFIG";
    private final static String TASK_DEFS = "TASK_DEFS";
    private final static String WORKFLOW_DEFS = "WORKFLOW_DEFS";
    private final static String EVENT_HANDLERS = "EVENT_HANDLERS";
    private Map<String, TaskDef> taskDefCache = new HashMap<>();

    @Inject
    public Elasticsearch6RestMetadataDAO(RestHighLevelClient client, Configuration config, ObjectMapper mapper) {
        super(client, config, mapper, "metadata");

        ensureIndexExists(toIndexName(CONFIG), toTypeName(CONFIG));
        ensureIndexExists(toIndexName(TASK_DEFS), toTypeName(TASK_DEFS));
        ensureIndexExists(toIndexName(WORKFLOW_DEFS), toTypeName(WORKFLOW_DEFS));
        ensureIndexExists(toIndexName(EVENT_HANDLERS), toTypeName(EVENT_HANDLERS));

        refreshTaskDefs();
        int cacheRefreshTime = config.getIntProperty("conductor.taskdef.cache.refresh.time.seconds", 60);
        Executors.newSingleThreadScheduledExecutor()
                .scheduleWithFixedDelay(this::refreshTaskDefs, cacheRefreshTime, cacheRefreshTime, TimeUnit.SECONDS);
    }

    @Override
    public String createTaskDef(TaskDef taskDef) {
        if (logger.isDebugEnabled())
            logger.debug("createTaskDef: taskDef={}", toJson(taskDef));
        taskDef.setCreateTime(System.currentTimeMillis());
        return insertOrUpdate(taskDef);
    }

    @Override
    public String updateTaskDef(TaskDef taskDef) {
        if (logger.isDebugEnabled())
            logger.debug("updateTaskDef: taskDef={}", toJson(taskDef));
        taskDef.setUpdateTime(System.currentTimeMillis());
        return insertOrUpdate(taskDef);
    }

    @Override
    public TaskDef getTaskDef(String name) {
        if (logger.isDebugEnabled())
            logger.debug("getTaskDef: name={}", name);
        Preconditions.checkNotNull(name, "TaskDef name cannot be null");
        TaskDef taskDef = taskDefCache.get(name);
        if (taskDef != null) {
            if (logger.isDebugEnabled())
                logger.debug("getTaskDef: found in cache={}", toJson(taskDef));
            return taskDef;
        }

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);
        String id = toId(name);

        taskDef = findOne(indexName, typeName, id, TaskDef.class);
        if (logger.isDebugEnabled())
            logger.debug("getTaskDef: result={}", toJson(taskDef));

        return taskDef;
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        if (logger.isDebugEnabled())
            logger.debug("getAllTaskDefs");

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);

        return findAll(indexName, typeName, TaskDef.class);
    }

    @Override
    public void removeTaskDef(String name) {
        if (logger.isDebugEnabled())
            logger.debug("removeTaskDef: name={}", name);

        Preconditions.checkNotNull(name, "TaskDef name cannot be null");
        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);
        String id = toId(name);

        delete(indexName, typeName, id);

        if (logger.isDebugEnabled())
            logger.debug("removeTaskDef: done");
    }

    @Override
    public void create(WorkflowDef workflowDef) {
        if (logger.isDebugEnabled())
            logger.debug("create: workflowDef={}", toJson(workflowDef));
        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);
        String id = toId(workflowDef.getName(), String.valueOf(workflowDef.getVersion()));
        if (exists(indexName, typeName, id)) {
            throw new ApplicationException(ApplicationException.Code.CONFLICT, "Workflow with " + workflowDef.key() + " already exists!");
        }
        workflowDef.setCreateTime(System.currentTimeMillis());
        insertOrUpdate(workflowDef);
    }

    @Override
    public void update(WorkflowDef workflowDef) {
        if (logger.isDebugEnabled())
            logger.debug("update: workflowDef={}", toJson(workflowDef));
        workflowDef.setUpdateTime(System.currentTimeMillis());
        insertOrUpdate(workflowDef);
    }

    @Override
    public void removeWorkflow(WorkflowDef workflowDef) {
        if (logger.isDebugEnabled())
            logger.debug("removeWorkflow: workflowDef={}", toJson(workflowDef));
        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);
        String id = toId(workflowDef.getName(), String.valueOf(workflowDef.getVersion()));
        delete(indexName, typeName, id);

        if (logger.isDebugEnabled())
            logger.debug("removeWorkflow: done");
    }

    @Override
    public WorkflowDef getLatest(String name) {
        if (logger.isDebugEnabled())
            logger.debug("getLatest: name={}", name);
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        List<WorkflowDef> items = getAllVersions(name);
        if (logger.isDebugEnabled())
            logger.debug("getLatest: items={}", toJson(items));
        if (items.isEmpty()) {
            return null;
        }

        items.sort(Comparator.comparingInt(WorkflowDef::getVersion).reversed());
        WorkflowDef workflowDef = items.get(0);
        if (logger.isDebugEnabled())
            logger.debug("getLatest: result={}", toJson(workflowDef));

        return workflowDef;
    }

    @Override
    public WorkflowDef get(String name, int version) {
        if (logger.isDebugEnabled())
            logger.debug("get: name={}, version={}", name, version);
        return getByVersion(name, String.valueOf(version));
    }

    @Override
    public List<String> findAll() {
        if (logger.isDebugEnabled())
            logger.debug("findAll");
        List<WorkflowDef> items = getAll();
        if (logger.isDebugEnabled())
            logger.debug("findAll: items={}", toJson(items));
        if (items.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> result = items.stream().map(WorkflowDef::getName).collect(Collectors.toList());
        if (logger.isDebugEnabled())
            logger.debug("findAll: result={}", toJson(result));
        return result;
    }

    @Override
    public List<WorkflowDef> getAll() {
        if (logger.isDebugEnabled())
            logger.debug("getAll");
        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        return findAll(indexName, typeName, WorkflowDef.class);
    }

    @Override
    public List<WorkflowDef> getAllLatest() {
        if (logger.isDebugEnabled())
            logger.debug("getAllLatest");
        List<String> names = findAll();
        if (names.isEmpty()) {
            return Collections.emptyList();
        }

        List<WorkflowDef> result = names.stream().map(this::getLatest).collect(Collectors.toList());
        if (logger.isDebugEnabled())
            logger.debug("getAllLatest: result={}", toJson(result));
        return result;
    }

    @Override
    public List<WorkflowDef> getAllVersions(String name) {
        if (logger.isDebugEnabled())
            logger.debug("getAllVersions: name={}", name);
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);

        QueryBuilder query = QueryBuilders.termQuery("name", name);
        List<WorkflowDef> result = findAll(indexName, typeName, query, WorkflowDef.class);

        if (logger.isDebugEnabled())
            logger.debug("getAllVersions: result={}", toJson(result));
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addEventHandler(EventHandler handler) {
        if (logger.isDebugEnabled())
            logger.debug("addEventHandler: eventHandler={}", toJson(handler));
        Preconditions.checkNotNull(handler.getName(), "Missing Name");

        EventHandler existing = getEventHandler(handler.getName());
        if (existing != null) {
            throw new ApplicationException(ApplicationException.Code.CONFLICT, "EventHandler with name " + handler.getName() + " already exists!");
        }

        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);
        String id = toId(handler.getName());

        insert(indexName, typeName, id, handler);

        if (logger.isDebugEnabled())
            logger.debug("addEventHandler: done");
    }

    @Override
    public void updateEventHandler(EventHandler eventHandler) {
        if (logger.isDebugEnabled())
            logger.debug("updateEventHandler: eventHandler={}", toJson(eventHandler));
        Preconditions.checkNotNull(eventHandler.getName(), "Missing Name");

        EventHandler existing = getEventHandler(eventHandler.getName());
        if (existing == null) {
            throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "EventHandler with name " + eventHandler.getName() + " not found!");
        }

        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);
        String id = toId(eventHandler.getName());

        update(indexName, typeName, id, eventHandler);

        if (logger.isDebugEnabled())
            logger.debug("updateEventHandler: done");
    }

    @Override
    public void removeEventHandlerStatus(String name) {
        if (logger.isDebugEnabled())
            logger.debug("removeEventHandlerStatus: name={}", name);

        EventHandler existing = getEventHandler(name);
        if (existing == null) {
            throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "EventHandler with name " + name + " not found!");
        }

        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);
        String id = toId(name);

        delete(indexName, typeName, id);

        if (logger.isDebugEnabled())
            logger.debug("removeEventHandlerStatus: done");
    }

    @Override
    public List<EventHandler> getEventHandlers() {
        if (logger.isDebugEnabled())
            logger.debug("getEventHandlers");
        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);

        return findAll(indexName, typeName, EventHandler.class);
    }

    @Override
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
        if (logger.isDebugEnabled())
            logger.debug("getEventHandlersForEvent: event={}, activeOnly={}", event, activeOnly);

        List<EventHandler> handlers = getEventHandlers();
        if (logger.isDebugEnabled())
            logger.debug("getEventHandlersForEvent: found={}", toJson(handlers));
        if (handlers.isEmpty()) {
            return Collections.emptyList();
        }

        handlers = handlers.stream().filter(eh -> eh.getEvent().equals(event) && (!activeOnly || eh.isActive()))
                .collect(Collectors.toList());
        if (logger.isDebugEnabled())
            logger.debug("getEventHandlersForEvent: result={}", toJson(handlers));

        return handlers;
    }

    private WorkflowDef getByVersion(String name, String version) {
        if (logger.isDebugEnabled())
            logger.debug("getByVersion: name={}, version={}", name, version);
        Preconditions.checkNotNull(name, "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);
        String id = toId(name, version);

        WorkflowDef workflowDef = findOne(indexName, typeName, id, WorkflowDef.class);

        if (logger.isDebugEnabled())
            logger.debug("getByVersion: result={}", toJson(workflowDef));
        return workflowDef;
    }

    private EventHandler getEventHandler(String name) {
        if (logger.isDebugEnabled())
            logger.debug("getEventHandler: name={}", name);
        String indexName = toIndexName(EVENT_HANDLERS);
        String typeName = toTypeName(EVENT_HANDLERS);
        String id = toId(name);

        EventHandler handler = findOne(indexName, typeName, id, EventHandler.class);

        if (logger.isDebugEnabled())
            logger.debug("getEventHandler: result={}", toJson(handler));
        return handler;
    }

    private String insertOrUpdate(TaskDef def) {
        if (logger.isDebugEnabled())
            logger.debug("insertOrUpdate: taskDef={}", toJson(def));
        Preconditions.checkNotNull(def, "TaskDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "TaskDef name cannot be null");

        String indexName = toIndexName(TASK_DEFS);
        String typeName = toTypeName(TASK_DEFS);
        String id = toId(def.getName());

        upsert(indexName, typeName, id, def);

        refreshTaskDefs();
        if (logger.isDebugEnabled())
            logger.debug("insertOrUpdate: done {}", def.getName());
        return def.getName();
    }

    private void insertOrUpdate(WorkflowDef def) {
        if (logger.isDebugEnabled())
            logger.debug("insertOrUpdate: workflowDef={}", toJson(def));
        Preconditions.checkNotNull(def, "WorkflowDef object cannot be null");
        Preconditions.checkNotNull(def.getName(), "WorkflowDef name cannot be null");

        String indexName = toIndexName(WORKFLOW_DEFS);
        String typeName = toTypeName(WORKFLOW_DEFS);
        String id = toId(def.getName(), String.valueOf(def.getVersion()));

        upsert(indexName, typeName, id, def);

        if (logger.isDebugEnabled())
            logger.debug("insertOrUpdate: done");
    }

    private void refreshTaskDefs() {
        if (logger.isDebugEnabled())
            logger.debug("refreshTaskDefs");

        Map<String, TaskDef> map = new HashMap<>();
        getAllTaskDefs().forEach(taskDef -> map.put(taskDef.getName(), taskDef));
        this.taskDefCache = map;

        if (logger.isDebugEnabled())
            logger.debug("refreshTaskDefs: task defs={}", map);
    }

    @Override
    public List<Pair<String, String>> getConfigs() {
        String indexName = toIndexName(CONFIG);
        String typeName = toTypeName(CONFIG);
        ensureIndexExists(indexName);

        List<Pair<String, String>> result = new LinkedList<>();
        try {

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            sourceBuilder.size(1000);

            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(indexName).types(typeName);
            searchRequest.source(sourceBuilder);
            searchRequest.scroll(scroll);

            SearchResponse searchResponse = client.search(searchRequest);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    String value = (String)hit.getSourceAsMap().get("value");
                    result.add(Pair.of(hit.getId(), value));
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest);

        } catch (Exception ex) {
            logger.error("getConfigs: failed for {}/{} with {}", indexName, typeName, ex.getMessage(), ex);
        }
        return result;
    }
}
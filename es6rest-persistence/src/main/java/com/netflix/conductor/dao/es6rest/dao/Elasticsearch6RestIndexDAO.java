package com.netflix.conductor.dao.es6rest.dao;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es6rest.parser.Expression;
import com.netflix.conductor.metrics.Monitors;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.netflix.conductor.dao.es6rest.dao.Elasticsearch6RestAbstractDAO.isVerConflictException;

/**
 * @author Viren
 */
@Trace
@Singleton
public class Elasticsearch6RestIndexDAO implements IndexDAO {
    private static final Logger log = LoggerFactory.getLogger(Elasticsearch6RestIndexDAO.class);
    private static final String className = Elasticsearch6RestIndexDAO.class.getSimpleName();
    private static final String WORKFLOW_DOC_TYPE = "workflow";
    private static final String TASK_DOC_TYPE = "task";
    private static final String LOG_DOC_TYPE = "task";
    private static final String EVENT_DOC_TYPE = "event";
    private static final String MSG_DOC_TYPE = "message";
    private static final String RESOURCE_TASK_LOG = "/es6conductor_task_log.json";
    private static final String RESOURCE_EXECUTIONS = "/es6conductor_executions.json";
    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMww");

    static {
        sdf.setTimeZone(gmt);
    }

    private RestClientBuilder builder;
    private String execWorkflowIndexName;
    private String execTaskIndexName;
    private String logMessageIndexName;
    private String logEventIndexName;
    private String logTaskIndexName;
    private String logIndexPrefix;
    private ObjectMapper om;

    private int retryDelay;

    @Inject
    public Elasticsearch6RestIndexDAO(RestClientBuilder builder, Configuration config, ObjectMapper om) {
        this.om = om;
        this.builder = builder;
        String rootIndexName = config.getProperty("workflow.elasticsearch.index.name", "conductor");
        String taskLogPrefix = config.getProperty("workflow.elasticsearch.tasklog.index.name", "task_log");

        String stack = config.getStack();
        this.logIndexPrefix = rootIndexName + "." + taskLogPrefix + "." + stack;
        this.execTaskIndexName = rootIndexName + ".executions." + stack + ".task";
        this.execWorkflowIndexName = rootIndexName + ".executions." + stack + ".workflow";
        this.retryDelay = config.getIntProperty("workflow.elasticsearch.retry.delay", 100);

        try {

            ensureIndexExists(execTaskIndexName, RESOURCE_EXECUTIONS, "task");
            ensureIndexExists(execWorkflowIndexName, RESOURCE_EXECUTIONS, "workflow");

            updateTaskLogIndexNames();
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::updateTaskLogIndexNames, 0, 1, TimeUnit.HOURS);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void ensureIndexExists(String name, String resourceFile, String type) {
        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            GetIndexRequest request = new GetIndexRequest().indices(name);
            boolean exists = client.indices().exists(request);
            if (exists) {
                return;
            }

            Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .build();

            InputStream stream = Elasticsearch6RestIndexDAO.class.getResourceAsStream(resourceFile);

            Map<String, Object> mapping = om.readValue(stream, new TypeReference<Map<String, Object>>() {
            });

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(name, settings)
                .mapping(type, (Map) mapping.get(type));

            client.indices().create(createIndexRequest);
        } catch (Exception ex) {
            if (!ex.getMessage().contains("index_already_exists_exception")) {
                log.error("ensureIndexExists failed for {} with {}", name, ex.getMessage(), ex);
            }
        }
    }

    private void updateTaskLogIndexNames() {
        String baseName = logIndexPrefix + "_" + sdf.format(new Date());

        ensureIndexExists(baseName + ".task", RESOURCE_TASK_LOG, "task");
        logTaskIndexName = baseName + ".task";

        ensureIndexExists(baseName + ".event", RESOURCE_TASK_LOG, "event");
        logEventIndexName = baseName + ".event";

        ensureIndexExists(baseName + ".message", RESOURCE_TASK_LOG, "message");
        logMessageIndexName = baseName + ".message";
    }

    @Override
    public void index(Workflow workflow) {
        try {

            String id = workflow.getWorkflowId();
            WorkflowSummary summary = new WorkflowSummary(workflow);
            byte[] doc = om.writeValueAsBytes(summary);

            UpdateRequest req = new UpdateRequest(execWorkflowIndexName, WORKFLOW_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            req.retryOnConflict(5);
            updateWithRetry(req);

        } catch (Throwable e) {
            log.error("Indexing failed {}", e.getMessage(), e);
        }
    }

    @Override
    public void index(Task task) {
        try {
            String id = task.getTaskId();
            TaskSummary summary = new TaskSummary(task);
            byte[] doc = om.writeValueAsBytes(summary);

            UpdateRequest req = new UpdateRequest(execTaskIndexName, TASK_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            updateWithRetry(req);

        } catch (Throwable e) {
            log.error("Indexing failed {}", e.getMessage(), e);
        }
    }

    @Override
    public void add(List<TaskExecLog> logs) {
        if (logs == null || logs.isEmpty()) {
            return;
        }

        int retry = 3;
        while (retry > 0) {
            try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
                BulkRequest brb = new BulkRequest();
                for (TaskExecLog log : logs) {
                    IndexRequest request = new IndexRequest(logTaskIndexName, LOG_DOC_TYPE);
                    request.source(om.writeValueAsBytes(log), XContentType.JSON);
                    brb.add(request);
                }
                BulkResponse response = client.bulk(brb);
                if (!response.hasFailures()) {
                    break;
                }
                retry--;

            } catch (Throwable e) {
                log.error("Indexing failed {}", e.getMessage(), e);
                retry--;
                if (retry > 0) {
                    Uninterruptibles.sleepUninterruptibly(retryDelay, TimeUnit.MILLISECONDS);
                }
            }
        }

    }

    @Override
    public List<TaskExecLog> getTaskLogs(String taskId) {
        List<TaskExecLog> logs = new LinkedList<>();
        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            QueryBuilder qf = QueryBuilders.matchAllQuery();
            Expression expression = Expression.fromString("taskId='" + taskId + "'");
            qf = expression.getFilterBuilder();

            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(qf);
            QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
            BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(fq);
            sourceBuilder.size(1000);

            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(logIndexPrefix + "*").types(TASK_DOC_TYPE);
            searchRequest.source(sourceBuilder);
            searchRequest.scroll(scroll);

            SearchResponse searchResponse = client.search(searchRequest);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    TaskExecLog tel = om.readValue(hit.getSourceAsString(), TaskExecLog.class);
                    logs.add(tel);
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

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return logs;
    }

    @Override
    public void addMessage(String queue, Message msg) {
        int retry = 3;
        while (retry > 0) {
            try (RestHighLevelClient client = new RestHighLevelClient(builder)) {

                Map<String, Object> doc = new HashMap<>();
                doc.put("messageId", msg.getId());
                doc.put("payload", msg.getPayload());
                doc.put("queue", queue);
                doc.put("created", System.currentTimeMillis());
                IndexRequest request = new IndexRequest(logMessageIndexName, MSG_DOC_TYPE);
                request.source(doc);
                client.index(request);
                break;

            } catch (Throwable e) {
                log.error("Indexing failed {}", e.getMessage(), e);
                retry--;
                if (retry > 0) {
                    Uninterruptibles.sleepUninterruptibly(retryDelay, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    @Override
    public void add(EventExecution ee) {
        try {
            byte[] doc = om.writeValueAsBytes(ee);
            String id = ee.getName() + "." + ee.getEvent() + "." + ee.getMessageId() + "." + ee.getId();
            UpdateRequest req = new UpdateRequest(logEventIndexName, EVENT_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            req.retryOnConflict(5);
            updateWithRetry(req);
        } catch (Throwable e) {
            log.error("Indexing failed {}", e.getMessage(), e);
        }
    }

    private void updateWithRetry(UpdateRequest request) {
        int retry = 3;
        while (retry > 0) {
            try (RestHighLevelClient client = new RestHighLevelClient(builder)) {

                client.update(request);
                return;

            } catch (Exception e) {
                if (isVerConflictException(e)) {
                    return;
                } else {
                    Monitors.error(className, "index");
                    log.error("Indexing failed for {}, {}: {}", request.index(), request.type(), e.getMessage(), e);
                    retry--;
                    if (retry > 0) {
                        Uninterruptibles.sleepUninterruptibly(retryDelay, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }
    }

    @Override
    public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count,
                                                List<String> sort, String from, String end) {
        try {
            return search(query, start, count, sort, freeText, from, end);
        } catch (Exception e) {
            throw new ApplicationException(ApplicationException.Code.BACKEND_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public void remove(String workflowId) {
        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {

            DeleteRequest req = new DeleteRequest(execWorkflowIndexName, WORKFLOW_DOC_TYPE, workflowId);
            DeleteResponse response = client.delete(req);
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                log.error("Index removal failed - document not found by id " + workflowId);
            }
        } catch (Throwable e) {
            log.error("Index removal failed failed {}", e.getMessage(), e);
            Monitors.error(className, "remove");
        }
    }

    @Override
    public void update(String workflowInstanceId, String[] keys, Object[] values) {
        if (keys.length != values.length) {
            throw new IllegalArgumentException("Number of keys and values should be same.");
        }

        UpdateRequest request = new UpdateRequest(execWorkflowIndexName, WORKFLOW_DOC_TYPE, workflowInstanceId);
        Map<String, Object> source = new HashMap<>();

        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            Object value = values[i];
            log.debug("updating {} with {} and {}", workflowInstanceId, key, value);
            source.put(key, value);
        }
        request.doc(source);
        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            client.update(request);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    @Override
    public String get(String workflowInstanceId, String fieldToGet) {
        Object value = null;
        GetRequest request = new GetRequest(execWorkflowIndexName, WORKFLOW_DOC_TYPE, workflowInstanceId).storedFields(fieldToGet);
        GetResponse response = null;
        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            response = client.get(request);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        Map<String, DocumentField> fields = response.getFields();
        if (fields == null) {
            return null;
        }
        DocumentField field = fields.get(fieldToGet);
        if (field != null) value = field.getValue();
        if (value != null) {
            return value.toString();
        }
        return null;
    }

    private SearchResult<String> search(String structuredQuery, int start, int size,
                                        List<String> sortOptions, String freeTextQuery,
                                        String from, String end) throws Exception {
        QueryBuilder qf = QueryBuilders.matchAllQuery();
        if (StringUtils.isNotEmpty(structuredQuery)) {
            Expression expression = Expression.fromString(structuredQuery);
            qf = expression.getFilterBuilder();
        }

        BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(qf);
        QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery(freeTextQuery);
        BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);
        if (StringUtils.isNotEmpty(from) && StringUtils.isNotEmpty(end)) {
            fq.must(QueryBuilders.rangeQuery("startTime").gte(from).lte(end));
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(fq);
        sourceBuilder.storedField("_id");
        sourceBuilder.from(start);
        sourceBuilder.size(size);

        if (sortOptions != null) {
            sortOptions.forEach(sortOption -> {
                SortOrder order = SortOrder.ASC;
                String field = sortOption;
                int indx = sortOption.indexOf(':');
                if (indx > 0) {    //Can't be 0, need the field name at-least
                    field = sortOption.substring(0, indx);
                    order = SortOrder.valueOf(sortOption.substring(indx + 1));
                }
                sourceBuilder.sort(field, order);
            });
        }

        SearchRequest searchRequest = new SearchRequest(execWorkflowIndexName).types(WORKFLOW_DOC_TYPE);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse;
        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            searchResponse = client.search(searchRequest);
        }
        SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.getTotalHits();

        List<String> result = new ArrayList<>(searchHits.getHits().length);
        searchHits.forEach(hit -> {
            result.add(hit.getId());
        });

        return new SearchResult<>(totalHits, result);
    }
}

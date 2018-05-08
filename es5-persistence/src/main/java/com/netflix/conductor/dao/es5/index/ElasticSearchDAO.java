/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.common.utils.RetryUtil;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es5.index.query.parser.Expression;
import com.netflix.conductor.dao.es5.index.query.parser.ParserException;
import com.netflix.conductor.metrics.Monitors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Viren
 */
@Trace
@Singleton
public class ElasticSearchDAO implements IndexDAO {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchDAO.class);

    private static final String WORKFLOW_DOC_TYPE = "workflow";

    private static final String TASK_DOC_TYPE = "task";

    private static final String LOG_DOC_TYPE = "task";

    private static final String EVENT_DOC_TYPE = "event";

    private static final String MSG_DOC_TYPE = "message";

    private static final String className = ElasticSearchDAO.class.getSimpleName();

    private static final int RETRY_COUNT = 3;

    private String indexName;

    private String logIndexName;

    private String logIndexPrefix;

    private ObjectMapper objectMapper;

    private Client elasticSearchClient;


    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyyMMww");

    private final ExecutorService executorService;

    static {
        SIMPLE_DATE_FORMAT.setTimeZone(GMT);
    }

    @Inject
    public ElasticSearchDAO(Client elasticSearchClient, Configuration config, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.elasticSearchClient = elasticSearchClient;
        this.indexName = config.getProperty("workflow.elasticsearch.index.name", null);

        try {

            initIndex();
            updateIndexName(config);
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> updateIndexName(config), 0, 1, TimeUnit.HOURS);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        int corePoolSize = 6;
        int maximumPoolSize = 12;
        long keepAliveTime = 1L;
        this.executorService = new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>());
    }

    private void updateIndexName(Configuration config) {
        this.logIndexPrefix = config.getProperty("workflow.elasticsearch.tasklog.index.name", "task_log");
        this.logIndexName = this.logIndexPrefix + "_" + SIMPLE_DATE_FORMAT.format(new Date());

        try {
            elasticSearchClient.admin().indices().prepareGetIndex().addIndices(logIndexName).execute().actionGet();
        } catch (IndexNotFoundException infe) {
            try {
                elasticSearchClient.admin().indices().prepareCreate(logIndexName).execute().actionGet();
            } catch (ResourceAlreadyExistsException ilee) {

            } catch (Exception e) {
                logger.error("Failed to update log index name: {}", logIndexName, e);
            }
        }
    }

    /**
     * Initializes the index with required templates and mappings.
     */
    private void initIndex() throws Exception {

        //0. Add the index template
        GetIndexTemplatesResponse result = elasticSearchClient.admin().indices().prepareGetTemplates("wfe_template").execute().actionGet();
        if(result.getIndexTemplates().isEmpty()) {
            logger.info("Creating the index template 'wfe_template'");
            InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/template.json");
            byte[] templateSource = IOUtils.toByteArray(stream);

            try {
                elasticSearchClient.admin().indices().preparePutTemplate("wfe_template").setSource(templateSource, XContentType.JSON).execute().actionGet();
            }catch(Exception e) {
                logger.error("Failed to init index template", e);
            }
        }

        //1. Create the required index
        try {
            elasticSearchClient.admin().indices().prepareGetIndex().addIndices(indexName).execute().actionGet();
        }catch(IndexNotFoundException infe) {
            try {
                elasticSearchClient.admin().indices().prepareCreate(indexName).execute().actionGet();
            }catch(ResourceAlreadyExistsException done) {}
        }

        //2. Mapping for the workflow document type
        GetMappingsResponse response = elasticSearchClient.admin().indices().prepareGetMappings(indexName).addTypes(WORKFLOW_DOC_TYPE).execute().actionGet();
        if(response.mappings().isEmpty()) {
            logger.info("Adding the workflow type mappings");
            InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/wfe_type.json");
            byte[] bytes = IOUtils.toByteArray(stream);
            String source = new String(bytes);
            try {
                elasticSearchClient.admin().indices().preparePutMapping(indexName).setType(WORKFLOW_DOC_TYPE).setSource(source).execute().actionGet();
            }catch(Exception e) {
                logger.error("Failed to init index mappings", e);
            }
        }
    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        try {

            String id = workflow.getWorkflowId();
            WorkflowSummary summary = new WorkflowSummary(workflow);
            byte[] doc = objectMapper.writeValueAsBytes(summary);

            UpdateRequest req = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            req.retryOnConflict(5);
            updateWithRetry(req, "Index workflow into doc_type workflow");

        } catch (Throwable e) {
            logger.error("Failed to index workflow: {}", workflow.getWorkflowId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
        return CompletableFuture.runAsync(() -> indexWorkflow(workflow), executorService);
    }

    @Override
    public void indexTask(Task task) {
        try {

            String id = task.getTaskId();
            TaskSummary summary = new TaskSummary(task);
            byte[] doc = objectMapper.writeValueAsBytes(summary);

            UpdateRequest req = new UpdateRequest(indexName, TASK_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            updateWithRetry(req, "Index workflow into doc_type workflow");

        } catch (Throwable e) {
            logger.error("Failed to index task: {}", task.getTaskId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncIndexTask(Task task) {
        return CompletableFuture.runAsync(() -> indexTask(task), executorService);
    }


    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs.isEmpty()) {
            return;
        }
        try {
            BulkRequestBuilder bulkRequestBuilder = elasticSearchClient.prepareBulk();
            for (TaskExecLog log : taskExecLogs) {
                IndexRequest request = new IndexRequest(logIndexName, LOG_DOC_TYPE);
                request.source(objectMapper.writeValueAsBytes(log), XContentType.JSON);
                bulkRequestBuilder.add(request);
            }
            new RetryUtil<BulkResponse>().retryOnException(() -> bulkRequestBuilder.execute().actionGet(), null ,
                    BulkResponse::hasFailures, RETRY_COUNT, "Indexing all execution logs into doc_type task", "addTaskExecutionLogs");
        } catch (Throwable e) {
            List<String> taskIds = taskExecLogs.stream().map(TaskExecLog::getTaskId).collect(Collectors.toList());
            logger.error("Failed to index task execution logs for tasks: ", taskIds, e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
        return CompletableFuture.runAsync(() -> addTaskExecutionLogs(logs), executorService);
    }

    @Override
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        try {
            Expression expression = Expression.fromString("taskId='" + taskId + "'");
            QueryBuilder queryBuilder = expression.getFilterBuilder();

            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(queryBuilder);
            QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
            BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

            final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(indexName).setQuery(fq).setTypes(TASK_DOC_TYPE);
            SearchResponse response = srb.execute().actionGet();
            SearchHit[] hits = response.getHits().getHits();
            List<TaskExecLog> logs = new ArrayList<>(hits.length);
            for(SearchHit hit : hits) {
                String source = hit.getSourceAsString();
                TaskExecLog tel = objectMapper.readValue(source, TaskExecLog.class);
                logs.add(tel);
            }

            return logs;

        }catch(Exception e) {
            logger.error("Failed to get task execution logs for task: {}", taskId, e);
        }

        return null;
    }

    @Override
    public void addMessage(String queue, Message message) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("messageId", message.getId());
        doc.put("payload", message.getPayload());
        doc.put("queue", queue);
        doc.put("created", System.currentTimeMillis());
        IndexRequest request = new IndexRequest(logIndexName, MSG_DOC_TYPE);
        request.source(doc);
        try {
            new RetryUtil<>().retryOnException(() -> elasticSearchClient.index(request).actionGet(), null,
                    null, RETRY_COUNT, "Indexing document in  for docType: message", "addMessage");
        } catch (Throwable e) {
            logger.error("Failed to index message: {}", message.getId(), e);
        }
    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
        try {
            byte[] doc = objectMapper.writeValueAsBytes(eventExecution);
            String id = eventExecution.getName() + "." + eventExecution.getEvent() + "." + eventExecution.getMessageId() + "." + eventExecution.getId();
            UpdateRequest req = new UpdateRequest(logIndexName, EVENT_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            req.retryOnConflict(5);
            updateWithRetry(req, "Update Event execution for doc_type event");
        } catch (Throwable e) {
            logger.error("Failed to index event execution: {}", eventExecution.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
        return CompletableFuture.runAsync(() -> addEventExecution(eventExecution), executorService);
    }

    private void updateWithRetry(UpdateRequest request, String operationDescription) {
        try {
            new RetryUtil<UpdateResponse>().retryOnException(() -> elasticSearchClient.update(request).actionGet(), null,
                    null, RETRY_COUNT,  operationDescription, "updateWithRetry");
        } catch (Exception e) {
            Monitors.error(className, "index");
            logger.error("Failed to index {} for request type: {}", request.index(), request.type(), e);
        }
    }

    @Override
    public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
        try {

            return search(query, start, count, sort, freeText, WORKFLOW_DOC_TYPE);

        } catch (ParserException e) {
            throw new ApplicationException(Code.BACKEND_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
        try {
            return search(query, start, count, sort, freeText, TASK_DOC_TYPE);
        } catch (ParserException e) {
            throw new ApplicationException(Code.BACKEND_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public void removeWorkflow(String workflowId) {
        try {
            DeleteRequest request = new DeleteRequest(indexName, WORKFLOW_DOC_TYPE, workflowId);
            DeleteResponse response = elasticSearchClient.delete(request).actionGet();
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                logger.error("Index removal failed - document not found by id: {}", workflowId);
            }
        } catch (Throwable e) {
            logger.error("Failed to remove workflow {} from index", workflowId, e);
            Monitors.error(className, "remove");
        }
    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        return CompletableFuture.runAsync(() -> removeWorkflow(workflowId), executorService);
    }

    @Override
    public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        if (keys.length != values.length) {
            throw new IllegalArgumentException("Number of keys and values should be same.");
        }

        UpdateRequest request = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, workflowInstanceId);
        Map<String, Object> source = IntStream.range(0, keys.length).boxed()
                .collect(Collectors.toMap(i -> keys[i], i -> values[i]));
        request.doc(source);
        logger.debug("Updating workflow {} with {}", workflowInstanceId, source);
        new RetryUtil<>().retryOnException(() -> elasticSearchClient.update(request), null, null, RETRY_COUNT,
                "Updating index for doc_type workflow", "updateWorkflow");
    }

    @Override
    public CompletableFuture<Void> asyncUpdateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        return CompletableFuture.runAsync(() -> updateWorkflow(workflowInstanceId, keys, values), executorService);
    }

    @Override
    public String get(String workflowInstanceId, String fieldToGet) {
        Object value = null;
        GetRequest request = new GetRequest(indexName, WORKFLOW_DOC_TYPE, workflowInstanceId).storedFields(fieldToGet);
        GetResponse response = elasticSearchClient.get(request).actionGet();
        Map<String, GetField> fields = response.getFields();
        if(fields == null) {
            return null;
        }
        GetField field = fields.get(fieldToGet);
        if(field != null) value = field.getValue();
        if(value != null) {
            return value.toString();
        }
        return null;
    }

    private SearchResult<String> search(String structuredQuery, int start, int size, List<String> sortOptions, String freeTextQuery, String docType) throws ParserException {
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        if(StringUtils.isNotEmpty(structuredQuery)) {
            Expression expression = Expression.fromString(structuredQuery);
            queryBuilder = expression.getFilterBuilder();
        }

        BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(queryBuilder);
        QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery(freeTextQuery);
        BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);
        final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(indexName).setQuery(fq).setTypes(docType).storedFields("_id").setFrom(start).setSize(size);
        if(sortOptions != null){
            sortOptions.forEach(sortOption -> {
                SortOrder order = SortOrder.ASC;
                String field = sortOption;
                int indx = sortOption.indexOf(':');
                if(indx > 0){    //Can't be 0, need the field name at-least
                    field = sortOption.substring(0, indx);
                    order = SortOrder.valueOf(sortOption.substring(indx+1));
                }
                srb.addSort(field, order);
            });
        }
        List<String> result = new LinkedList<String>();
        SearchResponse response = srb.get();
        response.getHits().forEach(hit -> {
            result.add(hit.getId());
        });
        long count = response.getHits().getTotalHits();
        return new SearchResult<String>(count, result);
    }
}

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
package com.netflix.conductor.dao.esrest.index;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.esrest.index.query.parser.Expression;
import com.netflix.conductor.dao.esrest.index.query.parser.ParserException;
import com.netflix.conductor.metrics.Monitors;

@Trace
@Singleton
public class ElasticSearchDAO implements IndexDAO {

	private static Logger log = LoggerFactory.getLogger(ElasticSearchDAO.class);
	
	private static final String WORKFLOW_DOC_TYPE = "workflow";
	
	private static final String TASK_DOC_TYPE = "task";
	
	private static final String LOG_DOC_TYPE = "task";
	
	private static final String EVENT_DOC_TYPE = "event";
	
	private static final String MSG_DOC_TYPE = "message";
	
	private static final String HTTP_VERB_GET = "GET";
	
	private static final String HTTP_VERB_PUT = "PUT";
	
	private static final String HTTP_VERB_POST = "POST";
	
	private static final String HTTP_VERB_DELETE = "DELETE";
	
	private static final String HTTP_VERB_HEAD = "HEAD";
	
	private static final int HTTP_STATUS_CODE_404 = 404;
	
	private static final String className = ElasticSearchDAO.class.getSimpleName();
	
	private String indexName;
	
	private String logIndexName;
	
	private String logIndexPrefix;

	private ObjectMapper om;
	
	private ElasticSearchRestClient client;
	
	
	private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
	    
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMww");

	
    static {
    	sdf.setTimeZone(gmt);
    }
	
	@Inject
	public ElasticSearchDAO(ElasticSearchRestClient client, Configuration config, ObjectMapper om) {
		this.om = om;
		this.client = client;
		this.indexName = config.getProperty("workflow.elasticsearch.index.name", null);
		
		try {
			initIndex();
			updateIndexName(config);
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> updateIndexName(config), 0, 1, TimeUnit.HOURS);
			
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
	
	// TESTED
	private void updateIndexName(Configuration config) {
		this.logIndexPrefix = config.getProperty("workflow.elasticsearch.tasklog.index.name", "task_log");
		this.logIndexName = this.logIndexPrefix + "_" + sdf.format(new Date());
		String path = "/" + this.logIndexName;
		try {
			client.getLowLevelClient().performRequest(HTTP_VERB_GET, path);
		} catch (ResponseException re) {
			try {
				client.getLowLevelClient().performRequest(HTTP_VERB_PUT, path);
			} catch (ResponseException re2) {
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
	
	/**
	 * Initializes the index with required templates and mappings.
	 */
	
	// TESTED
	private void initIndex() throws Exception {
		
		//0. Add the index template
		String templatePath = "/_template/wfe_template";
		Response headTemplateResponse = client.getLowLevelClient().performRequest(HTTP_VERB_HEAD, templatePath);
		//GetIndexTemplatesResponse result = client.admin().indices().prepareGetTemplates("wfe_template").execute().actionGet();
		if(headTemplateResponse.getStatusLine().getStatusCode() == HTTP_STATUS_CODE_404) {
			log.info("Creating the index template 'wfe_template'");
			InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/template.json");
			String templateSource = IOUtils.toString(stream);
			
			try {
				Map<String, String> params = Collections.emptyMap();
				HttpEntity entity = new NStringEntity(templateSource, ContentType.APPLICATION_JSON);
				client.getLowLevelClient().performRequest(HTTP_VERB_PUT, templatePath, params, entity);
			}catch(Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	
		//1. Create the required index
		String indexPath = "/" + indexName;
		try {
			client.getLowLevelClient().performRequest(HTTP_VERB_GET, indexPath);
		}catch(ResponseException re) {
			try {
				client.getLowLevelClient().performRequest(HTTP_VERB_PUT, indexPath);
			}catch(ResponseException re2) {}
		}
				
		//2. Mapping for the workflow document type
		String mappingPath = "/" + indexName + "/_mapping" + "/" + WORKFLOW_DOC_TYPE;
		try {
			client.getLowLevelClient().performRequest(HTTP_VERB_GET, mappingPath);

		} catch (ResponseException re) {
			log.info("Adding the workflow type mappings");
			InputStream stream = ElasticSearchDAO.class.getResourceAsStream("/wfe_type.json");
			String source = IOUtils.toString(stream);
			try {
				Map<String, String> params = Collections.emptyMap();
				HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
				client.getLowLevelClient().performRequest(HTTP_VERB_PUT, mappingPath, params, entity);
			}catch(Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	}
	
	// TESTED
	@Override
	public void index(Workflow workflow) {
		try {
			String id = workflow.getWorkflowId();
			WorkflowSummary summary = new WorkflowSummary(workflow);
			byte[] doc = om.writeValueAsBytes(summary);
			
			UpdateRequest req = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, id);
			req.doc(doc, XContentType.JSON);
			req.upsert(doc, XContentType.JSON);
			req.retryOnConflict(5);
			updateWithRetry(req);
 			
		} catch (Throwable e) {
			log.error("Indexing failed {}", e.getMessage(), e);
		}
	}
	
	//TESTED
	@Override
	public void index(Task task) {
		try {
			String id = task.getTaskId();
			TaskSummary summary = new TaskSummary(task);
			byte[] doc = om.writeValueAsBytes(summary);
			
			UpdateRequest req = new UpdateRequest(indexName, TASK_DOC_TYPE, id);
			req.doc(doc, XContentType.JSON);
			req.upsert(doc, XContentType.JSON);
			updateWithRetry(req);
 			
		} catch (Throwable e) {
			log.error("Indexing failed {}", e.getMessage(), e);
		}
	}
	

	@Override
	public void add(List<TaskExecLog> taskExecLogs) {
		
		if (taskExecLogs.isEmpty()) {
			return;
		}
		
		int retry = 3;
		while(retry > 0) {
			try {
				BulkRequest b = new BulkRequest();
//				BulkRequestBuilder brb = client.prepareBulk();
				for(TaskExecLog taskExecLog : taskExecLogs) {
					IndexRequest request = new IndexRequest(logIndexName, LOG_DOC_TYPE);
					request.source(om.writeValueAsBytes(taskExecLog), XContentType.JSON);
					b.add(request);
				}
				BulkResponse response = client.getHighLevelClient().bulk(b);
				if(!response.hasFailures()) {
					break;	
				}
				retry--;
	 			
			} catch (Throwable e) {
				log.error("Indexing failed {}", e.getMessage(), e);
				retry--;
				if(retry > 0) {
					Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
				}
			}
		}
		
	}

	@Override
	public List<TaskExecLog> getTaskLogs(String taskId) {
		try {

			QueryBuilder qf = QueryBuilders.matchAllQuery();
			Expression expression = Expression.fromString("taskId='" + taskId + "'");
			qf = expression.getFilterBuilder();

			BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(qf);
			QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
			BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

			SearchRequest sr = new SearchRequest(logIndexName);
			sr.types(TASK_DOC_TYPE);
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(fq);
			sr.source(sourceBuilder);
			SearchResponse response = client.getHighLevelClient().search(sr);
			SearchHit[] hits = response.getHits().getHits();
			List<TaskExecLog> logs = new ArrayList<>(hits.length);
			for(SearchHit hit : hits) {
				String source = hit.getSourceAsString();
				TaskExecLog tel = om.readValue(source, TaskExecLog.class);			
				logs.add(tel);
			}

			return logs;

		}catch(Exception e) {
			log.error(e.getMessage(), e);
		}

		return null;
	}

	@Override
	public void addMessage(String queue, Message msg) {
		int retry = 3;
		while(retry > 0) {
			try {
				
				Map<String, Object> doc = new HashMap<>();
				doc.put("messageId", msg.getId());
				doc.put("payload", msg.getPayload());
				doc.put("queue", queue);
				doc.put("created", System.currentTimeMillis());
				IndexRequest request = new IndexRequest(logIndexName, MSG_DOC_TYPE);
				request.source(doc);
	 			client.getHighLevelClient().index(request);
	 			break;
	 			
			} catch (Throwable e) {
				log.error("Indexing failed {}", e.getMessage(), e);
				retry--;
				if(retry > 0) {
					Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
				}
			}
		}
		
	}
	
	@Override
	public void add(EventExecution ee) {
		try {

			byte[] doc = om.writeValueAsBytes(ee);
			String id = ee.getName() + "." + ee.getEvent() + "." + ee.getMessageId() + "." + ee.getId();
			UpdateRequest req = new UpdateRequest(logIndexName, EVENT_DOC_TYPE, id);
			req.doc(doc, XContentType.JSON);
			req.upsert(doc, XContentType.JSON);
			req.retryOnConflict(5);
			updateWithRetry(req);
 			
		} catch (Throwable e) {
			log.error("Indexing failed {}", e.getMessage(), e);
		}
	
		
	}
	
	// TESTED
	private void updateWithRetry(UpdateRequest request) {
		int retry = 3;
		while(retry > 0) {
			try {
				
				client.getHighLevelClient().update(request);
				return;
				
			}catch(Exception e) {
				Monitors.error(className, "index");
				log.error("Indexing failed for {}, {}", request.index(), request.type(), e.getMessage());
				retry--;
				if(retry > 0) {
					Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
				}
			}
		}
	}
	
	// TESTED
	@Override
	public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
		try {
                        String query_mod;
                        // Append the query to check for non archived flows
                        query_mod = "(" + query + ")AND(archived!=\"true\")";
			return search(query_mod, start, count, sort, freeText, WORKFLOW_DOC_TYPE);
			
		} catch (ParserException e) {
			throw new ApplicationException(Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}
	
	// TESTED
	@Override
	public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
		try {
			return search(query, start, count, sort, freeText, TASK_DOC_TYPE);
			
		} catch (ParserException e) {
			throw new ApplicationException(Code.BACKEND_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public void remove(String workflowId) {
		try {

			DeleteRequest req = new DeleteRequest(indexName, WORKFLOW_DOC_TYPE, workflowId);
			DeleteResponse response = client.getHighLevelClient().delete(req);
			if (response.getResult() != DocWriteResponse.Result.DELETED) {
				log.error("Index removal failed - document not found by id " + workflowId);
			}
		} catch (Throwable e) {
			log.error("Index removal failed failed {}", e.getMessage(), e);
			Monitors.error(className, "remove");
		}
	}

        @Override
        public void removeTask(String taskId) {
                try {

                        DeleteRequest req = new DeleteRequest(indexName, TASK_DOC_TYPE, taskId);
                        DeleteResponse response = client.getHighLevelClient().delete(req);
                        if (response.getResult() != DocWriteResponse.Result.DELETED) {
                                log.error("Index removal failed - document not found by id " + taskId);
                        }
                } catch (Throwable e) {
                        log.error("Index removal failed failed {}", e.getMessage(), e);
                        Monitors.error(className, "remove");
                }
        }
	
	// TESTED
	@Override
	public void update(String workflowInstanceId, String[] keys, Object[] values) {
		if(keys.length != values.length) {
			throw new IllegalArgumentException("Number of keys and values should be same.");
		}
		
		UpdateRequest request = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, workflowInstanceId);
		Map<String, Object> source = new HashMap<>();

		for (int i = 0; i < keys.length; i++) {
			String key = keys[i];
			Object value= values[i];
			log.debug("updating {} with {} and {}", workflowInstanceId, key, value);
			source.put(key, value);
		}
		request.doc(source);
		try {
			client.getHighLevelClient().update(request);
		} catch (Exception e) {
			log.error("Error with update request for workflowInstanceId: " + workflowInstanceId);
			log.error(e.getMessage(), e);
		}
	}
	
	@Override
	public String get(String workflowInstanceId, String fieldToGet) {
		Object value = null;
		GetRequest request = new GetRequest(indexName, WORKFLOW_DOC_TYPE, workflowInstanceId).storedFields(fieldToGet);
		try {
			GetResponse response = client.getHighLevelClient().get(request);
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
		} catch (Exception e) {
			log.error("Error with get request for workflowInstanceId: " + workflowInstanceId);
			log.error(e.getMessage(), e);
			return null;
		}
	}
	
	// TESTED status="RUNNING"
	private SearchResult<String> search(String structuredQuery, int start, int size, List<String> sortOptions, String freeTextQuery, String docType) throws ParserException {
		QueryBuilder qf = QueryBuilders.matchAllQuery();
		if(StringUtils.isNotEmpty(structuredQuery)) {
			Expression expression = Expression.fromString(structuredQuery);
			qf = expression.getFilterBuilder();
		}
		
		BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(qf);
		QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery(freeTextQuery);
		BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);
		SearchRequest sr = new SearchRequest(indexName);
		sr.types(docType);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(fq);
		sourceBuilder.from(start);
		sourceBuilder.size(size);
		sourceBuilder.storedField("_id");
		if(sortOptions != null){
			sortOptions.forEach(sortOption -> {
				SortOrder order = SortOrder.ASC;
				String field = sortOption;
				int indx = sortOption.indexOf(':');
				if(indx > 0){	//Can't be 0, need the field name at-least
					field = sortOption.substring(0, indx);
					order = SortOrder.valueOf(sortOption.substring(indx+1));
				}
				sourceBuilder.sort(field, order);
			});
		}
		sr.source(sourceBuilder);
		List<String> result = new LinkedList<String>();
		try {
			SearchResponse response = client.getHighLevelClient().search(sr);
			response.getHits().forEach(hit -> {
				result.add(hit.getId());
			});
			long count = response.getHits().getTotalHits();
			return new SearchResult<String>(count, result);
		} catch (Exception e) {
			log.error("Error with search request");
			log.error(e.getMessage(), e);
			return new SearchResult<String>(0, null);
		}
	}

	
	
}

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
/**
 *
 */
package com.netflix.conductor.contribs.http;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * @author Viren
 * Task that enables calling another http endpoint as part of its execution
 */
@Singleton
public class HttpTask extends GenericHttpTask {
	private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);
	public static final String REQUEST_PARAMETER_NAME = "http_request";
	static final String MISSING_REQUEST = "Missing HTTP request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key wiht HttpTask.Input as value. See documentation for HttpTask for required input parameters";
	public static final String NAME = "HTTP";
	
	@Inject
	public HttpTask(RestClientManager rcm, Configuration config, ObjectMapper om) {
		super(NAME, config, rcm, om);
		logger.info("HttpTask initialized...");
	}
	
	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		Object request = task.getInputData().get(REQUEST_PARAMETER_NAME);
		task.setWorkerId(config.getServerId());
		String url = null;
		Input input = om.convertValue(request, Input.class);
		
		if (request == null) {
			task.setReasonForIncompletion(MISSING_REQUEST);
			task.setStatus(Status.FAILED);
			return;
		} else {
			if (input.getServiceDiscoveryQuery() != null) {
				url = lookup(input.getServiceDiscoveryQuery());
			}
		}
		
		if (input.getUri() == null) {
			task.setReasonForIncompletion("Missing HTTP URI. See documentation for HttpTask for required input parameters");
			task.setStatus(Status.FAILED);
			return;
		} else {
			if (url != null) {
				input.setUri(url + input.getUri());
			}
		}
		
		if (input.getMethod() == null) {
			task.setReasonForIncompletion("No HTTP method specified");
			task.setStatus(Status.FAILED);
			return;
		}
		
		try {
			HttpResponse response = new HttpResponse();
			logger.info("http task started.workflowId=" + workflow.getWorkflowId() + ",CorrelationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name=" + task.getReferenceTaskName() + ",request input=" + request);
			if (input.getContentType() != null) {
				if (input.getContentType().equalsIgnoreCase("application/x-www-form-urlencoded")) {
					String json = new ObjectMapper().writeValueAsString(task.getInputData());
					JSONObject obj = new JSONObject(json);
					JSONObject getSth = obj.getJSONObject("http_request");
					
					Object main_body = getSth.get("body");
					String body = main_body.toString();
					
					response = httpCallUrlEncoded(input, body);
					
				} else {
					response = httpCall(input);
				}
			} else {
				response = httpCall(input);
			}
			
			logger.info("http task execution completed.workflowId=" + workflow.getWorkflowId() + ",CorrelationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name=" + task.getReferenceTaskName() + ",response code=" + response.statusCode + ",response=" + response.body);
			if (response.statusCode > 199 && response.statusCode < 300) {
				task.setStatus(Status.COMPLETED);
			} else {
				if (response.body != null) {
					task.setReasonForIncompletion(response.body.toString());
				} else {
					task.setReasonForIncompletion("No response from the remote service");
				}
				task.setStatus(Status.FAILED);
			}
			if (response != null) {
				task.getOutputData().put("response", response.asMap());
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(e.getMessage());
			task.getOutputData().put("response", e.getMessage());
		}
	}
	
	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		return false;
	}
	
	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
	}
	
	@Override
	public boolean isAsync() {
		return true;
	}
	
	@Override
	public int getRetryTimeInSecond() {
		return 60;
	}
	
}

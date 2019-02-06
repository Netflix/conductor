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
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Viren
 * Task that enables calling another http endpoint as part of its execution
 */
@Singleton
public class HttpTask extends GenericHttpTask {
	private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);
	static final String MISSING_REQUEST = "Missing HTTP request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key wiht HttpTask.Input as value. See documentation for HttpTask for required input parameters";
	public static final String NAME = "HTTP";
	private static final String CONDITIONS_PARAMETER = "conditions";
	private int unackTimeout;

	@Inject
	public HttpTask(RestClientManager rcm, Configuration config, ObjectMapper om, AuthManager auth) {
		super(NAME, config, rcm, om, auth);
		unackTimeout = config.getIntProperty("workflow.system.task.http.unack.timeout", 60);
		logger.debug("HttpTask initialized...");
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
			if (StringUtils.isNotEmpty(input.getServiceDiscoveryQuery())) {
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
			logger.debug("http task started. workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskReferenceName=" + task.getReferenceTaskName() + ",url=" + input.getUri() + ",contextUser=" + workflow.getContextUser());
			if (input.getContentType() != null) {
				if (input.getContentType().equalsIgnoreCase("application/x-www-form-urlencoded")) {
					String json = new ObjectMapper().writeValueAsString(task.getInputData());
					JSONObject obj = new JSONObject(json);
					JSONObject getSth = obj.getJSONObject("http_request");

					Object main_body = getSth.get("body");
					String body = main_body.toString();

					response = httpCallUrlEncoded(input, body);

				} else {
					response = httpCall(input, task, workflow, executor);
				}
			} else {
				response = httpCall(input, task, workflow, executor);
			}

			logger.info("http task execution completed. workflowId=" + workflow.getWorkflowId() + ",correlationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskReferenceName=" + task.getReferenceTaskName() + ",url=" + input.getUri() + ",response code=" + response.statusCode + ",contextUser=" + workflow.getContextUser());

			// true - means status been handled, otherwise should apply the original logic
			boolean handled = handleStatusMapping(task, response);
			if (!handled) {
				handled = handleResponseMapping(task, response);
				if (!handled) {
					if (response.statusCode > 199 && response.statusCode < 300) {
						task.setStatus(Status.COMPLETED);
					} else {
						task.setStatus(Task.Status.FAILED);
					}
				}
			}

			// Check the http response validation. It will overwrite the task status if needed
			if (task.getStatus() == Status.COMPLETED) {
				checkHttpResponseValidation(task, response);
			} else {
				setReasonForIncompletion(response, task);
			}
			handleResetStartTime(task, executor);

			task.getOutputData().put("response", response.asMap());
		} catch (Exception ex) {
			logger.error("http task failed for workflowId=" + workflow.getWorkflowId()
					+ ",correlationId=" + workflow.getCorrelationId()
					+ ",contextUser=" + workflow.getContextUser()
					+ ",taskId=" + task.getTaskId()
					+ ",taskReferenceName=" + task.getReferenceTaskName()+ ",url=" + input.getUri() + " with " + ex.getMessage(), ex);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion(ex.getMessage());
			task.getOutputData().put("response", ex.getMessage());
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
		return unackTimeout;
	}

	@SuppressWarnings("unchecked")
	private void addEvalResult(Task task, String condition, Object result) {
		Map<String, Object> taskOutput = task.getOutputData();
		Map<String, Object> conditions = (Map<String, Object>) taskOutput.get(CONDITIONS_PARAMETER);
		if (conditions == null) {
			conditions = new HashMap<>();
			taskOutput.put(CONDITIONS_PARAMETER, conditions);
		}
		conditions.put(condition, result);
	}

	private void checkHttpResponseValidation(Task task, HttpResponse response) {
		Object responseParam = task.getInputData().get(RESPONSE_PARAMETER_NAME);

		// Check http_response object is present or not
		if (responseParam == null) {
			return;
		}
		Output output = om.convertValue(responseParam, Output.class);
		Validate validate = output.getValidate();

		// Check validate object is present or not
		if (validate == null) {
			return;
		}

		// Check condition object is present or not
		if (validate.getConditions() == null || validate.getConditions().isEmpty()) {
			return;
		}

		// Get the response map
		Map<String, Object> responseMap = response.asMap();

		// Go over all conditions and evaluate them
		AtomicBoolean overallStatus = new AtomicBoolean(true);
		validate.getConditions().forEach((name, condition) -> {
			try {
				Boolean success = ScriptEvaluator.evalBool(condition, responseMap);
				logger.debug("Evaluation resulted in " + success + " for " + name + "=" + condition);

				// Failed ?
				if (!success) {

					// Add condition evaluation result into output map
					addEvalResult(task, name, success);

					// Set the over all status to false
					overallStatus.set(false);
				}
			} catch (Exception ex) {
				logger.error("Evaluation failed for " + name + "=" + condition, ex);

				// Set the error message instead of false
				addEvalResult(task, name, ex.getMessage());

				// Set the over all status to false
				overallStatus.set(false);
			}
		});

		// If anything failed - fail the task
		if (!overallStatus.get()) {
			String overallReason = "Response validation failed";
			if (StringUtils.isNotEmpty(validate.getReason())) {
				overallReason = validate.getReason();
			}
			task.setReasonForIncompletion(overallReason);
			task.setStatus(Status.FAILED);
		}
	}
}

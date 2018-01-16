package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventProcessor;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.MetadataDAO;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;

/**
 * Created by pavanj on 6/22/17.
 */
public class HttpWaitTask extends GenericHttpTask {
	private static final Logger logger = LoggerFactory.getLogger(HttpWaitTask.class);
	private static final String HTTP_REQUEST_PARAM = "http_request";
	private static final String EVENT_WAIT_PARAM = "event_wait";
	private EventProcessor processor;
	private MetadataDAO metadata;

	@Inject
	public HttpWaitTask(Configuration config, RestClientManager rcm,
						ObjectMapper om, MetadataDAO metadata, EventProcessor processor) {
		super("HTTP_WAIT", config, rcm, om);
		this.processor = processor;
		this.metadata = metadata;

		logger.info("Http Event Wait Task initialized ...");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		logger.info("http wait task starting workflowId=" + workflow.getWorkflowId() + ",CorrelationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name=" + task.getReferenceTaskName());

		Object request = task.getInputData().get(HTTP_REQUEST_PARAM);
		if (request == null) {
			task.setReasonForIncompletion("Missing http request parameter");
			task.setStatus(Task.Status.FAILED);
			return;
		}

		String url = null;
		Input input = om.convertValue(request, Input.class);
		if (input.getServiceDiscoveryQuery() != null) {
			url = lookup(input.getServiceDiscoveryQuery());
		}

		if (StringUtils.isEmpty(input.getUri())) {
			task.setReasonForIncompletion("Missing http uri");
			task.setStatus(Task.Status.FAILED);
			return;
		} else {
			if (url != null) {
				input.setUri(url + input.getUri());
			}
		}

		if (StringUtils.isEmpty(input.getMethod())) {
			task.setReasonForIncompletion("Missing http method");
			task.setStatus(Task.Status.FAILED);
			return;
		}

		if (BooleanUtils.toBoolean(input.getTaskId())) {
			if (input.getBody() != null) {
				int index = input.getBody().toString().indexOf("}");
				if (input.getBody().toString().equals("{}")) {
					input.setBody("{taskid=" + task.getTaskId() + "}");
				} else {
					input.setBody(input.getBody().toString().substring(0, index) + ", taskid=" + task.getTaskId() + input.getBody().toString().substring(index));
				}
			} else {
				input.setBody("{taskid=" + task.getTaskId() + "}");
			}
		}

		if (BooleanUtils.toBoolean(input.getCurtimestamp())) {
			if (input.getBody() != null) {
				int index = input.getBody().toString().indexOf("}");
				if (input.getBody().toString().equals("{}")) {
					input.setBody("{Curtimestamp=" + System.currentTimeMillis() + "}");
				} else {
					input.setBody(input.getBody().toString().substring(0, index) + ", Curtimestamp=" + System.currentTimeMillis() + input.getBody().toString().substring(index));
				}
			} else {
				input.setBody("{Curtimestamp=" + System.currentTimeMillis() + "}");
			}
		}

		// register event handler right away as we should be ready to accept message
		if (!registerEventHandler(task)) {
			return;
		}

		try {
			HttpResponse response;
			logger.info("http wait task started.workflowId=" + workflow.getWorkflowId()
					+ ",CorrelationId=" + workflow.getCorrelationId()
					+ ",taskId=" + task.getTaskId()
					+ ",taskreference name=" + task.getReferenceTaskName() + ",request input=" + request);

			if (input.getContentType() != null) {
				if (input.getContentType().equalsIgnoreCase("application/x-www-form-urlencoded")) {
					Object bodyObjs = input.getBody();
					String bodyJson = om.writeValueAsString(bodyObjs);
					response = httpCallUrlEncoded(input, bodyJson);
				} else {
					response = httpCall(input, workflow);
				}
			} else {
				response = httpCall(input, workflow);
			}

			logger.info("http wait task execution completed.workflowId=" + workflow.getWorkflowId() + ",CorrelationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name=" + task.getReferenceTaskName() + ",response code=" + response.statusCode + ",response=" + response.body);
			if (response.statusCode > 199 && response.statusCode < 300) {
				task.setStatus(Task.Status.IN_PROGRESS);
			} else {
				if (response.body != null) {
					task.setReasonForIncompletion(response.body.toString());
				} else {
					task.setReasonForIncompletion("No response from the remote service");
				}
				task.setStatus(Task.Status.FAILED);
			}
			task.getOutputData().put("response", response.asMap());
		} catch (Exception ex) {
			logger.error("http wait task failed for workflowId=" + workflow.getWorkflowId()
					+ ",correlationId=" + workflow.getCorrelationId()
					+ ",taskId=" + task.getTaskId()
					+ ",taskreference name=" + task.getReferenceTaskName() + " with " + ex.getMessage(), ex);
			task.setStatus(Task.Status.FAILED);
			task.setReasonForIncompletion(ex.getMessage());
			task.getOutputData().put("response", ex.getMessage());
		}
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Task.Status.CANCELED);
	}

	@Override
	public boolean isAsync() {
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean registerEventHandler(Task task) {
		try {
			Map<String, Object> request = (Map<String, Object>) task.getInputData().get(EVENT_WAIT_PARAM);
			if (request == null) {
				task.setReasonForIncompletion("Missing '" + EVENT_WAIT_PARAM + "' request parameter");
				task.setStatus(Task.Status.FAILED);
				return false;
			}

			String event = (String) request.get("event");
			if (StringUtils.isEmpty(event)) {
				task.setReasonForIncompletion("Missing 'event' in '" + EVENT_WAIT_PARAM + "' request parameter");
				task.setStatus(Task.Status.FAILED);
				return false;
			}

			String workflowId = (String) request.get("workflowId");
			if (StringUtils.isEmpty(workflowId)) {
				task.setReasonForIncompletion("Missing 'workflowId' in '" + EVENT_WAIT_PARAM + "' request parameter");
				task.setStatus(Task.Status.FAILED);
				return false;
			}

			String taskId = (String) request.get("taskId");
			if (StringUtils.isEmpty(taskId)) {
				task.setReasonForIncompletion("Missing 'taskId' in '" + EVENT_WAIT_PARAM + "' request parameter");
				task.setStatus(Task.Status.FAILED);
				return false;
			}

			String status = (String) request.get("status");
			if (StringUtils.isEmpty(taskId)) {
				task.setReasonForIncompletion("Missing 'status' in '" + EVENT_WAIT_PARAM + "' request parameter");
				task.setStatus(Task.Status.FAILED);
				return false;
			}

			boolean resetStartTime = false;
			Object tmpObject = request.get("resetStartTime");
			if (tmpObject != null) {
				resetStartTime = Boolean.parseBoolean(tmpObject.toString());
			}

			EventHandler.UpdateTask updateTask = new EventHandler.UpdateTask();
			updateTask.setWorkflowId(workflowId);
			updateTask.setTaskId(taskId);
			updateTask.setStatus(status);
			updateTask.setResetStartTime(resetStartTime);
			updateTask.setFailedReason((String) request.get("failedReason"));
			updateTask.setStatuses((Map<String, String>) request.get("statuses"));
			updateTask.setOutput((Map<String, Object>) request.get("output"));

			EventHandler.Action action = new EventHandler.Action();
			action.setAction(EventHandler.Action.Type.update_task);
			action.setUpdate_task(updateTask);

			EventHandler handler = new EventHandler();
			handler.setCondition("true"); // We don't have specific conditions for that
			handler.setName(event);
			handler.setActive(true);
			handler.setEvent(event);
			handler.setActions(Collections.singletonList(action));

			try {
				metadata.addEventHandler(handler);
			} catch (ApplicationException ignore) {
			}

			// Start listener right away
			processor.refresh();
		} catch (Exception ex) {
			task.setReasonForIncompletion("Unable to register event handler: " + ex.getMessage());
			task.setStatus(Task.Status.FAILED);
			logger.error("registerEventHandler: failed with " + ex.getMessage() + " for " + task, ex);
			return false;
		}
		return true;
	}
}

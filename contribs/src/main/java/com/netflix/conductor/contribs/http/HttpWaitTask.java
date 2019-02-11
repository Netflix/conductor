package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.auth.AuthManager;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

/**
 * Created by pavanj on 6/22/17.
 */
public class HttpWaitTask extends GenericHttpTask {
	private static final Logger logger = LoggerFactory.getLogger(HttpWaitTask.class);
	private static final String EVENT_WAIT_PARAM = "event_wait";
	private EventProcessor processor;
	private MetadataDAO metadata;

	@Inject
	public HttpWaitTask(Configuration config, RestClientManager rcm, ObjectMapper om, MetadataDAO metadata,
			EventProcessor processor, AuthManager auth) {
		super("HTTP_WAIT", config, rcm, om, auth);
		this.processor = processor;
		this.metadata = metadata;

		logger.debug("Http Event Wait Task initialized ...");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		Object request = task.getInputData().get(REQUEST_PARAMETER_NAME);
		if (request == null) {
			task.setReasonForIncompletion("Missing http request parameter");
			task.setStatus(Task.Status.FAILED);
			return;
		}

		String hostAndPort = null;
		Input input = om.convertValue(request, Input.class);
		if (StringUtils.isNotEmpty(input.getServiceDiscoveryQuery())) {
			hostAndPort = lookup(input.getServiceDiscoveryQuery());
		}

		if (StringUtils.isEmpty(input.getUri())) {
			task.setReasonForIncompletion("Missing http uri");
			task.setStatus(Task.Status.FAILED);
			return;
		} else if (StringUtils.isNotEmpty(hostAndPort)) {
			final String uri = input.getUri();

			if (uri.startsWith("/")) {
				input.setUri(hostAndPort + uri);
			} else {
				// https://jira.d3nw.com/browse/ONECOND-837
				// Parse URI, extract the path, and append it to url
				try {
					URL tmp = new URL(uri);
					input.setUri(hostAndPort + tmp.getPath());
				} catch (MalformedURLException e) {
					logger.error("Unable to build endpoint URL: " + uri, e);
					throw new Exception("Unable to build endpoint URL: " + uri, e);
				}
			}
		} else if (StringUtils.isNotEmpty(input.getUri())) {
			// Do Nothing, use input.getUri() as is
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
					input.setBody(input.getBody().toString().substring(0, index) + ", taskid=" + task.getTaskId()
							+ input.getBody().toString().substring(index));
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
					input.setBody(input.getBody().toString().substring(0, index) + ", Curtimestamp="
							+ System.currentTimeMillis() + input.getBody().toString().substring(index));
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
			logger.debug("http wait task started.workflowId=" + workflow.getWorkflowId() + ",CorrelationId="
					+ workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name="
					+ task.getReferenceTaskName() + ",url=" + input.getUri() + ",contextUser=" + workflow.getContextUser());

			if (input.getContentType() != null) {
				if (input.getContentType().equalsIgnoreCase("application/x-www-form-urlencoded")) {
					Object bodyObjs = input.getBody();
					String bodyJson = om.writeValueAsString(bodyObjs);
					response = httpCallUrlEncoded(input, bodyJson);
				} else {
					response = httpCall(input, task, workflow, executor);
				}
			} else {
				response = httpCall(input, task, workflow, executor);
			}

			logger.info("http wait task execution completed.workflowId=" + workflow.getWorkflowId() + ",CorrelationId="
					+ workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name="
					+ task.getReferenceTaskName() + ",url=" + input.getUri() + ",response code=" + response.statusCode
					+ ",contextUser=" + workflow.getContextUser());

			// true - means status been handled, otherwise should apply the original logic
			boolean handled = handleStatusMapping(task, response);
			if (!handled) {
				if (response.statusCode > 199 && response.statusCode < 300) {
					task.setStatus(Task.Status.IN_PROGRESS);
				} else {
					task.setStatus(Task.Status.FAILED);
				}
			}
			// Check the http response validation. It will overwrite the task status if
			// needed
			if (task.getStatus() != Task.Status.IN_PROGRESS) {
				setReasonForIncompletion(response, task);
			}
			task.getOutputData().put("response", response.asMap());
		} catch (Exception ex) {
			logger.error("http wait task failed for workflowId=" + workflow.getWorkflowId() + ",correlationId="
					+ workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name="
					+ task.getReferenceTaskName() + ",url=" + input.getUri() + ",contextUser=" + workflow.getContextUser()
					+ " with " + ex.getMessage(), ex);
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

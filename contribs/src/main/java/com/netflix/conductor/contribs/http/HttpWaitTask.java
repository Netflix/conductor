package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

/**
 * Created by pavanj on 6/22/17.
 */
public class HttpWaitTask extends GenericHttpTask {
	private static final Logger logger = LoggerFactory.getLogger(HttpWaitTask.class);
	private static final String HTTP_REQUEST_PARAM = "http_request";
	private static final String EVENT_MESSAGES_PARAM = "event_messages";
	private static final String URN_WORKFLOW_ID = "urn:deluxe:conductor:workflow";
	private static final String URN_TASK_ID = "urn:deluxe:conductor:task";
	private static final String PROPERTY_QUEUE = "conductor.http.wait.event.queue.name";
	private static final TypeReference HASH_MAP_TYPE_REF = new TypeReference<HashMap<String, Object>>() { };
	private static final Map<String, Task.Status> STATUS_MAP = new HashMap<>();
	private WorkflowExecutor executor;
	private ObservableQueue queue;

	static {
		STATUS_MAP.put("failed", Task.Status.FAILED);
		STATUS_MAP.put("pending", Task.Status.IN_PROGRESS);
		STATUS_MAP.put("complete", Task.Status.COMPLETED);
		STATUS_MAP.put("cancelled", Task.Status.FAILED);
		STATUS_MAP.put("in-progress", Task.Status.IN_PROGRESS);
	}

	@Inject
	public HttpWaitTask(Configuration config, RestClientManager rcm, ObjectMapper om, WorkflowExecutor executor) {
		super("HTTP_WAIT", config, rcm, om);
		this.executor = executor;

		String name = config.getProperty(PROPERTY_QUEUE, null);
		logger.info("Event queue name is " + name);
		if (name == null) {
			throw new RuntimeException("No event status queue defined");
		}

		queue = EventQueues.getQueue(name, false);
		if (queue == null) {
			throw new RuntimeException("Unable to find queue by name " + name);
		}

		queue.observe().subscribe((Message msg) -> onMessage(queue, msg));
		logger.info("Http Event Wait Task initialized ...");
	}

	@Override
	@SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		logger.info("http wait task starting workflowId=" + workflow.getWorkflowId() + ",CorrelationId=" + workflow.getCorrelationId() + ",taskId=" + task.getTaskId() + ",taskreference name=" + task.getReferenceTaskName());

		// send nomad command
		Map<String, ?> request = (Map<String, ?>) task.getInputData().get(HTTP_REQUEST_PARAM);
		if (request == null) {
			task.setReasonForIncompletion("Missing HTTP request. Task input MUST have a '" + HTTP_REQUEST_PARAM + "' key with HttpWaitTask as value");
			task.setStatus(Task.Status.FAILED);
			return;
		}

		String url = null;
		Input input = om.convertValue(request, Input.class);
		if (input.getServiceDiscoveryQuery() != null) {
			url = lookup(input.getServiceDiscoveryQuery());
		}

		if (input.getUri() == null) {
			task.setReasonForIncompletion("Missing HTTP URI. See documentation for HttpTask for required input parameters");
			task.setStatus(Task.Status.FAILED);
			return;
		} else {
			if (url != null) {
				input.setUri(url + input.getUri());
			}
		}

		if (input.getMethod() == null) {
			task.setReasonForIncompletion("No HTTP method specified");
			task.setStatus(Task.Status.FAILED);
			return;
		}

		try {
			HttpResponse response = new HttpResponse();
			logger.info("http wait task started.workflowId=" + workflow.getWorkflowId()
					+ ",CorrelationId=" + workflow.getCorrelationId()
					+ ",taskId=" + task.getTaskId()
					+ ",taskreference name=" + task.getReferenceTaskName() + ",request input=" + request);

			if (input.getContentType() != null) {
				if (input.getContentType().equalsIgnoreCase("application/x-www-form-urlencoded")) {
					Object bodyObjs = request.get("body");
					String bodyJson = om.writeValueAsString(bodyObjs);
					response = httpCallUrlEncoded(input, bodyJson);
				} else {
					response = httpCall(input);
				}
			} else {
				response = httpCall(input);
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

	private void onMessage(ObservableQueue queue, Message msg) {
		String payload = null;
		try {
			payload = msg.getPayload();
			logger.info("Received payload " + payload + " for " + queue.getURI());

			Map<String, Object> event = om.readValue(payload, HASH_MAP_TYPE_REF);

			String workflowId = getUrn(event, URN_WORKFLOW_ID);
			if (StringUtils.isEmpty(workflowId)) {
				throw new RuntimeException("No '" + URN_WORKFLOW_ID + "' value exists in the event");
			}

			String taskId = getUrn(event, URN_TASK_ID);
			if (StringUtils.isEmpty(taskId)) {
				throw new RuntimeException("No '" + URN_TASK_ID + "' value exists in the event");
			}

			Workflow workflow = executor.getWorkflow(workflowId, true);
			if (workflow == null) {
				throw new RuntimeException("No workflow found with id " + workflowId);
			}

			Task targetTask = workflow.getTasks().stream().filter(item -> taskId.equals(item.getTaskId()))
					.findFirst().orElse(null);
			if (targetTask == null) {
				throw new RuntimeException("No task found with id " + taskId + " for workflow " + workflowId);
			}

			targetTask.getOutputData().put("event", event);

			// TODO Handle status from mapping
			Task.Status status = STATUS_MAP.get(getStatus(event));
			targetTask.setStatus(status);

			// Create task update wrapper and reset timer if in-progress
			TaskResult taskResult = new TaskResult(targetTask);
			if (Task.Status.IN_PROGRESS.equals(status)) {
				taskResult.setResetStartTime(true);
			}

			// Let's update task
			executor.updateTask(taskResult);

		} catch (Exception ex) {
			logger.error("Unable to process event: " + ex.getMessage() + " for payload " + payload, ex);
		}
	}

	private String getUrn(Map<String, Object> output, String key) {
		Object urnObj = output.get("Urns");
		if (urnObj == null) {
			throw new RuntimeException("No 'Urns' object found in the event " + output);
		}

		if (!(urnObj instanceof List)) {
			throw new RuntimeException("Wrong 'Urns' type. Expected List, got " + urnObj.getClass().getSimpleName());
		}

		for (Object item : (List<?>) urnObj) {
			if (item instanceof String) {
				String temp = (String) item;
				if (temp.startsWith(key)) {
					return temp.replaceAll(key + ":", "");
				}
			}
		}

		return null;
	}

	private String getStatus(Map<String, Object> input) {
		ParametersUtils pu = new ParametersUtils();
		pu.replace(input, "");

		Object statusObj = input.get("Status");
		if (statusObj == null) {
			throw new RuntimeException("No 'Status' object found in the event " + input);
		}
		if (!(statusObj instanceof Map)) {
			throw new RuntimeException("Wrong 'Status' type. Expected Map, got " + statusObj.getClass().getSimpleName());
		}

		return (String) ((Map) statusObj).get("Name");
	}
}

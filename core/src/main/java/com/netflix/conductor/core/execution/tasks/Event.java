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
package com.netflix.conductor.core.execution.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.correlation.Correlator;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Viren
 *
 */
public class Event extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(Event.class);
	
	private final ObjectMapper om = new ObjectMapper();
	private final ParametersUtils pu = new ParametersUtils();
	private final boolean useGroupId;


	public static final String NAME = "EVENT";
	private static final String JOB_ID_URN_PREFIX = "urn:deluxe:one-orders:deliveryjob:";

	@Inject
	public Event(Configuration config) {
		super(NAME);
		this.useGroupId = Boolean.parseBoolean(config.getProperty("io.shotgun.use.groupId.header", "false"));
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {
		
		Map<String, Object> payload = new HashMap<>();
		payload.putAll(task.getInputData());
		payload.put("workflowInstanceId", workflow.getWorkflowId());
		payload.put("workflowType", workflow.getWorkflowType());
		payload.put("workflowVersion", workflow.getVersion());
		payload.put("correlationId", workflow.getCorrelationId());
		
		String payloadJson = om.writeValueAsString(payload);
		Message message = new Message(task.getTaskId(), payloadJson, task.getTaskId());
		message.setTraceId(workflow.getTraceId());
		if (useGroupId) {
			message.setHeaders(new HashMap<String, String>(){{
				String jmsxGroupId = getJMXGroupId(workflow);
				logger.info("Conductor Event Message header JMSXGroupID " + jmsxGroupId);
				put("JMSXGroupID", jmsxGroupId);
			}});
		}

		ObservableQueue queue = getQueue(workflow, task);
		if(queue != null) {
			try {
				queue.publish(Arrays.asList(message));
				task.getOutputData().putAll(payload);
				task.setStatus(Status.COMPLETED);

				provider.addEventPublished(queue, message);
			} catch (Exception ex) {
				logger.error(ex.getMessage(), ex);
				task.setStatus(Status.FAILED);
				task.setReasonForIncompletion(ex.getMessage());
			}
			logger.debug("Event message published.Message="+message.getPayload()+",status=" + workflow.getStatus()+",workflowId="+workflow.getWorkflowId()+",correlationId="+workflow.getCorrelationId() + ",contextUser=" + workflow.getContextUser());
		} else {
			task.setReasonForIncompletion("No queue found to publish.");
			logger.error("No queue found to publish.");
			task.setStatus(Status.FAILED);
		}

	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {
		return false;
	}
	
	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor provider) throws Exception {
		Message message = new Message(task.getTaskId(), null, task.getTaskId());
		getQueue(workflow, task).ack(Arrays.asList(message));
	}

	@VisibleForTesting
	ObservableQueue getQueue(Workflow workflow, Task task) {
		
		String sinkValueRaw = "" + task.getInputData().get("sink");
		Map<String, Object> input = new HashMap<>();
		input.put("sink", sinkValueRaw);
		Map<String, Object> replaced = pu.getTaskInputV2(input, workflow, task.getTaskId(), null);
		String sinkValue = (String)replaced.get("sink");
		
		String queueName = sinkValue;

		if(sinkValue.startsWith("conductor")) {
			
			if("conductor".equals(sinkValue)) {
				
				queueName = sinkValue + ":" + workflow.getWorkflowType() + ":" + task.getReferenceTaskName();
				
			} else if(sinkValue.startsWith("conductor:")) {
				
				queueName = sinkValue.replaceAll("conductor:", "");
				queueName = "conductor:" + workflow.getWorkflowType() + ":" + queueName;
				
			} else {
				task.setStatus(Status.FAILED);
				task.setReasonForIncompletion("Invalid / Unsupported sink specified: " + sinkValue);
				logger.error("Invalid / Unsupported sink specified: " + sinkValue);
				return null;
			}
			
		}

		task.getOutputData().put("event_produced", queueName);
		
		try {
			return EventQueues.getQueue(queueName, true);
		}catch(Exception e) {
			logger.error("Error when trying to access the specified queue/topic: " + sinkValue + ", error: " + e.getMessage());
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion("Error when trying to access the specified queue/topic: " + sinkValue + ", error: " + e.getMessage());
			return null;			
		}
		
	}
	
	@Override
	public boolean isAsync() {
		return false;
	}

	private String getJMXGroupId(Workflow workflow){
		String jmsxGroupId = workflow.getWorkflowId();
		String correlationId = workflow.getCorrelationId();
		if ( StringUtils.isNotEmpty(correlationId)){
			String jobId = getJobId(correlationId);
			if ( StringUtils.isNotEmpty(jobId)){
				logger.info("Conductor Event Message: Getting Job ID " + jobId);
				jmsxGroupId = jobId;
			}
		}
		return jmsxGroupId;
	}

	private String getJobId(String correlationId) {
		Correlator correlator = new Correlator(logger, correlationId);
		String jobIdUrn = correlator.getContext().getUrn(JOB_ID_URN_PREFIX);
		if (StringUtils.isNotEmpty(jobIdUrn))
			return jobIdUrn.substring(JOB_ID_URN_PREFIX.length());
		return null;
	}

}

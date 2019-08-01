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
package com.netflix.conductor.contribs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventPublished;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.TaskStatusListener;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@Singleton
public class StatusEventPublisher implements TaskStatusListener, WorkflowStatusListener {
	private static Logger logger = LoggerFactory.getLogger(StatusEventPublisher.class);

	public enum StartEndState {
		start, end
	}

	private ParametersUtils pu = new ParametersUtils();
	private MetadataDAO metadata;
	private ExecutionDAO edao;
	private ObjectMapper om;

	@Inject
	public StatusEventPublisher(MetadataDAO metadata, ExecutionDAO edao, ObjectMapper om) {
		this.metadata = metadata;
		this.edao = edao;
		this.om = om;
	}

	@Override
	public void onTaskStarted(Task task) {
		notifyTaskStatus(task, StartEndState.start);
	}

	@Override
	public void onTaskFinished(Task task) {
		notifyTaskStatus(task, StartEndState.end);
	}

	@Override
	public void onWorkflowStarted(Workflow workflow) {
		notifyWorkflowStatus(workflow, StartEndState.start);
	}

	@Override
	public void onWorkflowCompleted(Workflow workflow) {
		notifyWorkflowStatus(workflow, StartEndState.end);
	}

	@Override
	public void onWorkflowTerminated(Workflow workflow) {
		notifyWorkflowStatus(workflow, StartEndState.end);
	}

	@SuppressWarnings("unchecked")
	private void notifyWorkflowStatus(Workflow workflow, StartEndState state) {
		try {
			WorkflowDef workflowDef = metadata.get(workflow.getWorkflowType(), workflow.getVersion());
			Map<String, Object> eventMap = workflowDef.getEventMessages();
			if (eventMap == null || !eventMap.containsKey(state.name())) {
				return;
			}

			// Get the 'start' or 'end' map
			eventMap = (Map<String, Object>) eventMap.get(state.name());

			// Check preProcess map for JSON Path engine
			Map<String, Object> preProcess = (Map<String, Object>) eventMap.get("defaults");
			if (MapUtils.isNotEmpty(preProcess)) {
				// Generate variables input
				Map<String, Map<String, Object>> inputMap = pu.getInputMap(null, workflow, null, null);

				// Replace preProcess map
				preProcess = pu.replace(preProcess, inputMap);
			}

			// Feed preProcessed map as defaults so that already processed for JQ engine
			Map<String, Map<String, Object>> defaults = Collections.singletonMap("defaults", preProcess);
			Map<String, Object> doc = pu.getTaskInputV2(eventMap, defaults, workflow, null, null, null);
			sendMessage(doc);
		} catch (Exception ex) {
			logger.debug("Unable to notify workflow status " + state.name() + ", failed with " + ex.getMessage(), ex);
			throw new RuntimeException(ex.getMessage(), ex);
		}
	}

	@SuppressWarnings("unchecked")
	private void notifyTaskStatus(Task task, StartEndState state) {
		try {
			Map<String, Object> eventMap = task.getWorkflowTask().getEventMessages();
			if (eventMap == null || !eventMap.containsKey(state.name())) {
				return;
			}

			// Get the 'start' or 'end' map
			eventMap = (Map<String, Object>) eventMap.get(state.name());

			Workflow workflow = edao.getWorkflow(task.getWorkflowInstanceId());

			// Check preProcess map for JSON Path engine
			Map<String, Object> preProcess = (Map<String, Object>) eventMap.get("defaults");
			if (MapUtils.isNotEmpty(preProcess)) {
				// Generate variables input
				Map<String, Map<String, Object>> inputMap = pu.getInputMap(null, workflow, null, null);

				// Replace preProcess map
				preProcess = pu.replace(preProcess, inputMap);
			}

			// Feed preProcessed map as defaults so that already processed for JQ engine
			Map<String, Map<String, Object>> defaults = Collections.singletonMap("defaults", preProcess);
			Map<String, Object> doc = pu.getTaskInputV2(eventMap, defaults, workflow, task.getTaskId(), null, null);
			sendMessage(doc);
		} catch (Exception ex) {
			logger.debug("Unable to notify task status " + state.name() + ", failed with " + ex.getMessage(), ex);
			throw new RuntimeException(ex.getMessage(), ex);
		}
	}

	private void sendMessage(Map<String, Object> actionMap) throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		Message msg = new Message();
		msg.setId(UUID.randomUUID().toString());

		String payload = mapper.writeValueAsString(actionMap.get("inputParameters"));
		msg.setPayload(payload);

		String sink = (String) actionMap.get("sink");
		ObservableQueue queue = EventQueues.getQueue(sink, false);
		if (queue == null) {
			logger.debug("sendMessage. No queue found for " + sink);
			return;
		}

		queue.publish(Collections.singletonList(msg));

		addEventPublished(queue, msg);
	}

	private void addEventPublished(ObservableQueue queue, Message msg) {
		try {
			Map<String, Object> payload = om.readValue(msg.getPayload(), new TypeReference<Map<String, Object>>() {
			});

			String subject = queue.getURI();
			if (queue.getURI().contains(":")) {
				subject = queue.getURI().substring(0, queue.getURI().indexOf(':'));
			}

			EventPublished ep = new EventPublished();
			ep.setId(msg.getId());
			ep.setSubject(subject);
			ep.setPayload(payload);
			ep.setType(queue.getType());
			ep.setPublished(System.currentTimeMillis());

			edao.addEventPublished(ep);
		} catch (Exception ex) {
			logger.debug("addEventPublished failed with " + ex.getMessage() +
				" for queue uri=" + queue.getURI() + ", payload=" + msg.getPayload());
		}
	}
}

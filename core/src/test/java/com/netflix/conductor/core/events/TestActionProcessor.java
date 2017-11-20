/**
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.conductor.core.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.service.MetadataService;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Viren
 *
 */
public class TestActionProcessor {
	private ObjectMapper om = new ObjectMapper();

	@Test
	public void testAray() throws Exception {
		ActionProcessor ap = new ActionProcessor(null, null);
		ParametersUtils pu = new ParametersUtils();

		List<Object> list = new LinkedList<>();
		Map<String, Object> map = new HashMap<>();
		map.put("externalId", "[{\"taskRefName\":\"t001\",\"workflowId\":\"w002\"}]");
		map.put("name", "conductor");
		map.put("version", 2);
		list.add(map);

		int before = list.size();
		ap.expand(list);
		assertEquals(before, list.size());


		Map<String, Object> input = new HashMap<>();
		input.put("k1", "${$..externalId}");
		input.put("k2", "${$[0].externalId[0].taskRefName}");
		input.put("k3", "${__json_externalId.taskRefName}");
		input.put("k4", "${$[0].name}");
		input.put("k5", "${$[0].version}");

		Map<String, Object> replaced = pu.replace(input, list);
		assertNotNull(replaced);
		System.out.println(replaced);

		assertEquals(replaced.get("k2"), "t001");
		assertNull(replaced.get("k3"));
		assertEquals(replaced.get("k4"), "conductor");
		assertEquals(replaced.get("k5"), 2);
	}

	@Test
	public void testMap() throws Exception {
		ActionProcessor ap = new ActionProcessor(null, null);
		ParametersUtils pu = new ParametersUtils();


		Map<String, Object> map = new HashMap<>();
		map.put("externalId", "{\"taskRefName\":\"t001\",\"workflowId\":\"w002\"}");
		map.put("name", "conductor");
		map.put("version", 2);

		ap.expand(map);


		Map<String, Object> input = new HashMap<>();
		input.put("k1", "${$.externalId}");
		input.put("k2", "${externalId.taskRefName}");
		input.put("k4", "${name}");
		input.put("k5", "${version}");

		//Map<String, Object> replaced = pu.replace(input, new ObjectMapper().writeValueAsString(map));
		Map<String, Object> replaced = pu.replace(input, map);
		assertNotNull(replaced);
		System.out.println("testMap(): " + replaced);

		assertEquals("t001", replaced.get("k2"));
		assertNull(replaced.get("k3"));
		assertEquals("conductor", replaced.get("k4"));
		assertEquals(2, replaced.get("k5"));
	}

	@Test
	public void testNoExpand() throws Exception {
		ParametersUtils pu = new ParametersUtils();


		Map<String, Object> map = new HashMap<>();
		map.put("name", "conductor");
		map.put("version", 2);
		map.put("externalId", "{\"taskRefName\":\"t001\",\"workflowId\":\"w002\"}");

		Map<String, Object> input = new HashMap<>();
		input.put("k1", "${$.externalId}");
		input.put("k4", "${name}");
		input.put("k5", "${version}");

		ObjectMapper om = new ObjectMapper();
		Object jsonObj = om.readValue(om.writeValueAsString(map), Object.class);

		Map<String, Object> replaced = pu.replace(input, jsonObj);
		assertNotNull(replaced);
		System.out.println("testNoExpand(): " + replaced);

		assertEquals("{\"taskRefName\":\"t001\",\"workflowId\":\"w002\"}", replaced.get("k1"));
		assertEquals("conductor", replaced.get("k4"));
		assertEquals(2, replaced.get("k5"));
	}

	@Test
	public void updateTask_evaluation_fail() throws Exception {
		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata);

		EventHandler.UpdateTask updateTask = new EventHandler.UpdateTask();
		updateTask.setWorkflowId(".workflowId");
		updateTask.setTaskId(".taskId");
		updateTask.setStatus(".status");
		updateTask.setResetStartTime(true);

		EventHandler.Action action = new EventHandler.Action();
		action.setAction(EventHandler.Action.Type.update_task);
		action.setUpdate_task(updateTask);

		Map<String, Object> payload = new HashMap<>();

		// workflowId evaluating is empty
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("workflowId evaluating is empty", op.get("error"));

		//taskId evaluating is empty
		payload.put("workflowId", "1");
		op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("taskId evaluating is empty", op.get("error"));

		//status evaluating is empty
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("status evaluating is empty", op.get("error"));

		//failedReason evaluating is empty
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "completed");
		action.getUpdate_task().setFailedReason(".failedReason");
		op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("failedReason evaluating is empty", op.get("error"));

		//Unable to determine task status. 1
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "x");
		payload.put("failedReason", "dummy");
		action.getUpdate_task().setFailedReason(".failedReason");
		op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("Unable to determine task status", op.get("error"));

		//Unable to determine task status. 2
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "done");
		payload.put("failedReason", "dummy");
		action.getUpdate_task().setFailedReason(".failedReason");
		action.getUpdate_task().getStatuses().put("done", "bad value");
		op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("Unable to determine task status", op.get("error"));
	}

	@Test
	public void updateTask_no_workflow() throws Exception {
		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata);

		EventHandler.Action action = newUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "completed");

		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("No workflow found with id 1", op.get("error"));
	}

	@Test
	public void updateTask_no_task() throws Exception {
		WorkflowExecutor executor = mock(WorkflowExecutor.class);

		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		when(executor.getWorkflow("1", true)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata);

		EventHandler.Action action = newUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "completed");

		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), "1", "2");
		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals(action.getUpdate_task(), op.get("action"));
		assertEquals("1", op.get("conductor.event.name"));
		assertEquals("2", op.get("conductor.event.messageId"));
		assertEquals("No task found with id 2 for workflow 1", op.get("error"));
	}

	@Test
	public void updateTask_completed() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		Task task = new Task();
		task.setTaskId("2");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getWorkflow("1", true)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata);

		EventHandler.Action action = newUpdateAction();
		action.getUpdate_task().getOutput().put("key", "value");

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "completed");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), "foo", "bar");
		verify(executor, times(1)).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals("value", captor.getValue().getOutputData().get("key"));
		assertEquals(TaskResult.Status.COMPLETED, captor.getValue().getStatus());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals("value", op.get("key"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void updateTask_failed() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		Task task = new Task();
		task.setTaskId("2");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getWorkflow("1", true)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata);

		EventHandler.Action action = newUpdateAction();
		action.getUpdate_task().setFailedReason(".failedReason");

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "failed");
		payload.put("failedReason", "reason");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), "foo", "bar");
		verify(executor, times(1)).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals(TaskResult.Status.FAILED, captor.getValue().getStatus());
		assertEquals("reason", captor.getValue().getReasonForIncompletion());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void updateTask_in_progress() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		Task task = new Task();
		task.setTaskId("2");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getWorkflow("1", true)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata);

		EventHandler.Action action = newUpdateAction();
		action.getUpdate_task().setResetStartTime(true);

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "in_progress");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), "foo", "bar");
		verify(executor, times(1)).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals(TaskResult.Status.IN_PROGRESS, captor.getValue().getStatus());
		assertTrue(captor.getValue().isResetStartTime());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	private EventHandler.Action newUpdateAction() {
		EventHandler.UpdateTask updateTask = new EventHandler.UpdateTask();
		updateTask.setWorkflowId(".workflowId");
		updateTask.setTaskId(".taskId");
		updateTask.setStatus(".status");
		updateTask.setResetStartTime(true);

		EventHandler.Action action = new EventHandler.Action();
		action.setAction(EventHandler.Action.Type.update_task);
		action.setUpdate_task(updateTask);

		return action;
	}
}
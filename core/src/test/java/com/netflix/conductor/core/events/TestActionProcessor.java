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
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.service.MetadataService;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Viren
 *
 */
public class TestActionProcessor {
	private ObjectMapper om = new ObjectMapper();
	private EventExecution ee = new EventExecution();

	public TestActionProcessor() {
		ee.setEvent("foo");
		ee.setMessageId("bar");
	}

	@Test
	public void testAray() throws Exception {
		ActionProcessor ap = new ActionProcessor(null, null, null);
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
		ActionProcessor ap = new ActionProcessor(null, null, null);
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
	public void updateTask_completed_id() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		Task task = new Task();
		task.setTaskId("2");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getWorkflow("1", true)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newUpdateActionId();
		action.getUpdate_task().getOutput().put("key", "value");

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "completed");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
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
		assertEquals(true, op.get("conductor.event.success"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void updateTask_completed_ref() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		Task task = new Task();
		task.setTaskId("2");
		task.setReferenceTaskName("ref2");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getWorkflow("1", true)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newUpdateActionRef();
		action.getUpdate_task().getOutput().put("key", "value");

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskRef", "ref2");
		payload.put("status", "completed");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
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
		assertEquals(true, op.get("conductor.event.success"));
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
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newUpdateActionId();
		action.getUpdate_task().setFailedReason(".failedReason");

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "failed");
		payload.put("failedReason", "reason");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(1)).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals(TaskResult.Status.FAILED, captor.getValue().getStatus());
		assertEquals("reason", captor.getValue().getReasonForIncompletion());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals(true, op.get("conductor.event.success"));
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
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newUpdateActionId();
		action.getUpdate_task().setResetStartTime(true);
		action.getUpdate_task().getStatuses().put("wip", "in_progress");

		Map<String, Object> payload = new HashMap<>();
		payload.put("workflowId", "1");
		payload.put("taskId", "2");
		payload.put("status", "wip");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals(TaskResult.Status.IN_PROGRESS, captor.getValue().getStatus());
		assertTrue(captor.getValue().isResetStartTime());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals(true, op.get("conductor.event.success"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void findUpdate_completed() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setWorkflowInstanceId("1");
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getPendingSystemTasks("WAIT")).thenReturn(Collections.singletonList(task));
		when(executor.getWorkflow("1", false)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(1)).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals(TaskResult.Status.COMPLETED, captor.getValue().getStatus());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals(true, op.get("conductor.event.success"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void findUpdate_failed() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setWorkflowInstanceId("1");
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getPendingSystemTasks("WAIT")).thenReturn(Collections.singletonList(task));
		when(executor.getWorkflow("1", false)).thenReturn(workflow);

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();
		action.getFind_update().setStatus(".status");
		action.getFind_update().getStatuses().put("failure", "FAILED");
		action.getFind_update().setFailedReason(".reason");

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");
		payload.put("status", "failure");
		payload.put("reason", "exception message");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(1)).updateTask(captor.capture());

		assertEquals(payload, captor.getValue().getOutputData().get("conductor.event.payload"));
		assertEquals("foo", captor.getValue().getOutputData().get("conductor.event.name"));
		assertEquals("bar", captor.getValue().getOutputData().get("conductor.event.messageId"));
		assertEquals(TaskResult.Status.FAILED, captor.getValue().getStatus());
		assertEquals("exception message", captor.getValue().getReasonForIncompletion());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals(true, op.get("conductor.event.success"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void findUpdate_empty_expression() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();
		action.getFind_update().getInputParameters().put("versionId", null);

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals("versionId expression is empty", op.get("error"));
		assertEquals(action.getFind_update(), op.get("action"));
	}

	@Test
	public void findUpdate_evaluating_empty() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();
		action.getFind_update().getInputParameters().put("versionId", ".FAKE");

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals(false, op.get("conductor.event.success"));
		assertNull(op.get("error"));
		assertNull(op.get("action"));
	}

	@Test
	public void findUpdate_evaluating_failed() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();
		action.getFind_update().getInputParameters().put("versionId", "WRONG");

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals("versionId evaluating failed with Function WRONG/0 does not exist", op.get("error"));
		assertEquals(action.getFind_update(), op.get("action"));
	}

	@Test
	public void findUpdate_empty_status() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();
		action.getFind_update().setStatus(".FAKE");

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertEquals(payload, op.get("conductor.event.payload"));
		assertEquals("foo", op.get("conductor.event.name"));
		assertEquals("bar", op.get("conductor.event.messageId"));
		assertEquals("Unable to determine status. Check mapping and payload", op.get("error"));
		assertEquals(action.getFind_update(), op.get("action"));
	}


	@Test
	public void findUpdate_skip_wf() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.COMPLETED);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertNull(op.get("error"));
		assertNull(op.get("action"));
		assertNotNull(op.get("conductor.event.payload"));
		assertNotNull(op.get("conductor.event.name"));
		assertNotNull(op.get("conductor.event.messageId"));
		assertEquals(4, op.size());
	}

	@Test
	public void findUpdate_skip_task_status() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.COMPLETED);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertNull(op.get("error"));
		assertNull(op.get("action"));
		assertNotNull(op.get("conductor.event.payload"));
		assertNotNull(op.get("conductor.event.name"));
		assertNotNull(op.get("conductor.event.messageId"));
		assertEquals(4, op.size());
	}

	@Test
	public void findUpdate_skip_task_type() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType("FAKE");
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		task.getInputData().put("versionId", "v");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertNull(op.get("error"));
		assertNull(op.get("action"));
		assertNotNull(op.get("conductor.event.payload"));
		assertNotNull(op.get("conductor.event.name"));
		assertNotNull(op.get("conductor.event.messageId"));
		assertEquals(4, op.size());
	}

	@Test
	public void findUpdate_skip_task_no_input() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertNull(op.get("error"));
		assertNull(op.get("action"));
		assertNotNull(op.get("conductor.event.payload"));
		assertNotNull(op.get("conductor.event.name"));
		assertNotNull(op.get("conductor.event.messageId"));
		assertEquals(4, op.size());
	}

	@Test
	public void findUpdate_skip_task_input_do_not_match() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "X");
		task.getInputData().put("versionId", "X");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertNull(op.get("error"));
		assertNull(op.get("action"));
		assertNotNull(op.get("conductor.event.payload"));
		assertNotNull(op.get("conductor.event.name"));
		assertNotNull(op.get("conductor.event.messageId"));
		assertEquals(4, op.size());
	}

	@Test
	public void findUpdate_skip_task_input_partial_match() throws Exception {
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		workflow.setWorkflowType("junit");
		workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

		Task task = new Task();
		task.setTaskId("2");
		task.setTaskType(Wait.NAME);
		task.setStatus(Task.Status.IN_PROGRESS);
		task.getInputData().put("featureId", "f");
		workflow.getTasks().add(task);

		WorkflowExecutor executor = mock(WorkflowExecutor.class);
		when(executor.getRunningWorkflows("junit")).thenReturn(Collections.singletonList(workflow));

		MetadataService metadata = mock(MetadataService.class);
		ActionProcessor ap = new ActionProcessor(executor, metadata, null);

		EventHandler.Action action = newFindUpdateAction();

		Map<String, Object> payload = new HashMap<>();
		payload.put("featureId", "f");
		payload.put("versionId", "v");

		ArgumentCaptor<TaskResult> captor = ArgumentCaptor.forClass(TaskResult.class);
		Map<String, Object> op = ap.execute(action, om.writeValueAsString(payload), ee);
		verify(executor, times(0)).updateTask(captor.capture());

		assertNull(op.get("error"));
		assertNull(op.get("action"));
		assertNotNull(op.get("conductor.event.payload"));
		assertNotNull(op.get("conductor.event.name"));
		assertNotNull(op.get("conductor.event.messageId"));
		assertEquals(4, op.size());
	}

	private EventHandler.Action newFindUpdateAction() {
		EventHandler.FindUpdate findUpdate = new EventHandler.FindUpdate();
		findUpdate.getInputParameters().put("featureId", ".featureId");
		findUpdate.getInputParameters().put("versionId", ".versionId");

		EventHandler.Action action = new EventHandler.Action();
		action.setAction(EventHandler.Action.Type.find_update);
		action.setFind_update(findUpdate);

		return action;
	}

	private EventHandler.Action newUpdateActionId() {
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

	private EventHandler.Action newUpdateActionRef() {
		EventHandler.UpdateTask updateTask = new EventHandler.UpdateTask();
		updateTask.setWorkflowId(".workflowId");
		updateTask.setTaskRef(".taskRef");
		updateTask.setStatus(".status");
		updateTask.setResetStartTime(true);

		EventHandler.Action action = new EventHandler.Action();
		action.setAction(EventHandler.Action.Type.update_task);
		action.setUpdate_task(updateTask);

		return action;
	}

}

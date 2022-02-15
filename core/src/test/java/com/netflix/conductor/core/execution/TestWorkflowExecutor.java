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
package com.netflix.conductor.core.execution;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask.Type;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.dao.ErrorLookupDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.ws.rs.core.HttpHeaders;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * @author Viren
 *
 */
public class TestWorkflowExecutor {

	WorkflowStatusListener workflowListener = mock(WorkflowStatusListener.class);
	TaskStatusListener taskListener = mock(TaskStatusListener.class);
	TestConfiguration config = new TestConfiguration();
	MetadataDAO metadata = mock(MetadataDAO.class);
	ExecutionDAO edao = mock(ExecutionDAO.class);
	ErrorLookupDAO errorLookupDAO = mock(ErrorLookupDAO.class);
	QueueDAO queue = mock(QueueDAO.class);
	ObjectMapper om = new ObjectMapper();
	AuthManager auth = mock(AuthManager.class);

	@Test
	public void test() throws Exception {
		
		AtomicBoolean httpTaskExecuted = new AtomicBoolean(false);
		AtomicBoolean http2TaskExecuted = new AtomicBoolean(false);
	
		new Wait();
		new WorkflowSystemTask("HTTP") {
			@Override
			public boolean isAsync() {
				return true;
			}
			
			@Override
			public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
				httpTaskExecuted.set(true);
				task.setStatus(Status.COMPLETED);
				super.start(workflow, task, executor);
			}
			
		};
		
		new WorkflowSystemTask("HTTP2") {
			
			@Override
			public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
				http2TaskExecuted.set(true);
				task.setStatus(Status.COMPLETED);
				super.start(workflow, task, executor);
			}
			
		};
		
		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");
		
		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO, om, auth, config,
				taskListener, workflowListener);
		List<Task> tasks = new LinkedList<>();
		
		WorkflowTask taskToSchedule = new WorkflowTask();
		taskToSchedule.setWorkflowTaskType(Type.USER_DEFINED);
		taskToSchedule.setType("HTTP");
		
		WorkflowTask taskToSchedule2 = new WorkflowTask();
		taskToSchedule2.setWorkflowTaskType(Type.USER_DEFINED);
		taskToSchedule2.setType("HTTP2");
		
		WorkflowTask wait = new WorkflowTask();
		wait.setWorkflowTaskType(Type.WAIT);
		wait.setType("WAIT");
		wait.setTaskReferenceName("wait");
		
		Task task1 = SystemTask.userDefined(workflow, IDGenerator.generate(), taskToSchedule, new HashMap<>(), null, 0);
		Task task2 = SystemTask.waitTask(workflow, IDGenerator.generate(), taskToSchedule, new HashMap<>());
		Task task3 = SystemTask.userDefined(workflow, IDGenerator.generate(), taskToSchedule2, new HashMap<>(), null, 0);
		
		tasks.add(task1);
		tasks.add(task2);
		tasks.add(task3);
		
		
		when(edao.createTasks(tasks)).thenReturn(tasks);
		AtomicInteger startedTaskCount = new AtomicInteger(0);
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				startedTaskCount.incrementAndGet();
				return null;
			}
		}).when(edao).updateTask(any());

		AtomicInteger queuedTaskCount = new AtomicInteger(0);		
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				String queueName = invocation.getArgumentAt(0, String.class);
				System.out.println(queueName);
				queuedTaskCount.incrementAndGet();
				return null;
			}
		}).when(queue).push(any(), any(), anyInt());

		boolean stateChanged = executor.scheduleTask(workflow, tasks);
		assertEquals(2, startedTaskCount.get());
		assertEquals(1, queuedTaskCount.get());
		assertTrue(stateChanged);
		assertFalse(httpTaskExecuted.get());
		assertTrue(http2TaskExecuted.get());
	}

	@Test(expected = NullPointerException.class)
	public void inputValidationSuccess() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validationSuccess");
		def.setVersion(1);
		def.setInputValidation(new HashMap<>());
		def.getInputValidation().put("rule1", "$.constant == 'value'");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validationSuccess", 1)).thenReturn(def);

		Map<String, Object> input = new HashMap<>();
		input.put("constant", "value");

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO, om, auth, config,
			taskListener, workflowListener);
		executor.startWorkflow("validationSuccess", 1, null, input);
	}

	@Test
	public void inputValidationFailure1() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validationFailure1");
		def.setVersion(1);
		def.setInputValidation(new HashMap<>());
		def.getInputValidation().put("rule2", "$.constant != undefined && $.constant == 'value'");

		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validationFailure1", 1)).thenReturn(def);

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO, om, auth, config,
			taskListener, workflowListener);
		try {
			executor.startWorkflow("validationFailure1", 1, null, new HashMap<>());
		} catch (ApplicationException ex) {
			assertEquals(ApplicationException.Code.INVALID_INPUT, ex.getCode());
			assertEquals("Input validation failed for 'rule2' rule", ex.getMessage());
		}
	}

	@Test
	public void inputValidationFailure2() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validationFailure2");
		def.setVersion(1);
		def.setInputValidation(new HashMap<>());
		def.getInputValidation().put("rule3", "wrong syntax");

		Workflow workflow = new Workflow();
		workflow.setWorkflowId("1");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validationFailure2", 1)).thenReturn(def);

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO,  om, auth, config,
			taskListener, workflowListener);
		try {
			executor.startWorkflow("validationFailure2", 1, null, new HashMap<>());
		} catch (ApplicationException ex) {
			assertEquals(ApplicationException.Code.INVALID_INPUT, ex.getCode());
			assertEquals("Input validation failed for 'rule3' rule: <eval>:1:6 Expected ; but found syntax\n" +
					"wrong syntax\n" +
					"      ^ in <eval> at line number 1 at column number 6", ex.getMessage());
		}
	}

	@Test
	public void authValidationSuccess() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validation");
		def.setVersion(1);
		def.getAuthValidation().put("rule1", ".access == \"foo\"");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validation", 1)).thenReturn(def);

		String token = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() + 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		HttpHeaders headers = mock(HttpHeaders.class);
		when(headers.getRequestHeader(HttpHeaders.AUTHORIZATION)).thenReturn(Collections.singletonList("Bearer " + token));

		Configuration cfg = mock(Configuration.class);
		when(cfg.getProperty("workflow.auth.validate", "false")).thenReturn("true");
		when(cfg.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/success");
		when(cfg.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(cfg.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");

		AuthManager manager = new AuthManager(cfg);

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO, om, manager, cfg,
			taskListener, workflowListener);
		executor.validateAuth(def, headers);
	}

	@Test(expected = ApplicationException.class)
	public void authValidationFailed() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validation");
		def.setVersion(1);
		def.getAuthValidation().put("rule1", ".access == \"wrong\"");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validation", 1)).thenReturn(def);

		String token = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() + 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		HttpHeaders headers = mock(HttpHeaders.class);
		when(headers.getRequestHeader(HttpHeaders.AUTHORIZATION)).thenReturn(Collections.singletonList("Bearer " + token));

		Configuration cfg = mock(Configuration.class);
		when(cfg.getProperty("workflow.auth.validate", "false")).thenReturn("true");
		when(cfg.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/success");
		when(cfg.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(cfg.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");

		AuthManager manager = new AuthManager(cfg);

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO,  om, manager, cfg,
			taskListener, workflowListener);
		executor.validateAuth(def, headers);
	}

	@Test
	public void authValidationNoHeader() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validation");
		def.setVersion(1);
		def.getAuthValidation().put("rule1", ".access == \"wrong\"");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validation", 1)).thenReturn(def);

		HttpHeaders headers = mock(HttpHeaders.class);

		Configuration cfg = mock(Configuration.class);
		when(cfg.getProperty("workflow.auth.validate", "false")).thenReturn("true");

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO,  om, auth, cfg,
			taskListener, workflowListener);
		try {
			executor.validateAuth(def, headers);
			fail("Should not be here");
		} catch (Exception ex) {
			assertEquals("No " + HttpHeaders.AUTHORIZATION + " header provided", ex.getMessage());
		}
	}

	@Test
	public void authValidationNoHeader2() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validation");
		def.setVersion(1);
		def.getAuthValidation().put("rule1", ".access == \"wrong\"");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validation", 1)).thenReturn(def);

		HttpHeaders headers = mock(HttpHeaders.class);
		when(headers.getRequestHeader(HttpHeaders.AUTHORIZATION)).thenReturn(Collections.singletonList(""));

		Configuration cfg = mock(Configuration.class);
		when(cfg.getProperty("workflow.auth.validate", "false")).thenReturn("true");

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao,  queue,errorLookupDAO,  om, auth, cfg,
			taskListener, workflowListener);
		try {
			executor.validateAuth(def, headers);
			fail("Should not be here");
		} catch (Exception ex) {
			assertEquals("No " + HttpHeaders.AUTHORIZATION + " header provided", ex.getMessage());
		}
	}

	@Test
	public void authValidationInvalidHeader() throws Exception {
		WorkflowDef def = new WorkflowDef();
		def.setName("validation");
		def.setVersion(1);
		def.getAuthValidation().put("rule1", ".access == \"wrong\"");

		MetadataDAO metadata = mock(MetadataDAO.class);
		when(metadata.get("validation", 1)).thenReturn(def);

		HttpHeaders headers = mock(HttpHeaders.class);
		when(headers.getRequestHeader(HttpHeaders.AUTHORIZATION)).thenReturn(Collections.singletonList("Bad start"));


		Configuration cfg = mock(Configuration.class);
		when(cfg.getProperty("workflow.auth.validate", "false")).thenReturn("true");

		WorkflowExecutor executor = new WorkflowExecutor(metadata, edao, queue, errorLookupDAO, om, auth, cfg,
			taskListener, workflowListener);
		try {
			executor.validateAuth(def, headers);
			fail("Should not be here");
		} catch (Exception ex) {
			assertEquals("Invalid " + HttpHeaders.AUTHORIZATION + " header format", ex.getMessage());
		}
	}
}

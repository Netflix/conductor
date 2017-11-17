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
package com.netflix.conductor.contribs.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Oleksiy Lysak
 *
 */
@SuppressWarnings("unchecked")
public class TestAuthTask {
	private WorkflowExecutor executor = mock(WorkflowExecutor.class);
	private Workflow workflow = new Workflow();
	private static Server server;
	private static ObjectMapper om = new ObjectMapper();
	private AuthTask authTask;
	private String accessToken;

	@BeforeClass
	public static void init() throws Exception {
		server = new Server(7010);
		ServletContextHandler servletContextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
		servletContextHandler.setHandler(new EchoHandler());
		server.start();
	}

	@AfterClass
	public static void cleanup() {
		if(server != null) {
			try {
				server.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Before
	public void setup() throws Exception {
		accessToken = Resources.toString(Resources.getResource("jwt.txt"), Charsets.UTF_8);

		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/success");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");
		authTask = new AuthTask(config);
	}

	@Test
	public void no_conductor_auth_url() throws Exception {
		Configuration config = mock(Configuration.class);
		AuthTask authTask = new AuthTask(config);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Missing system property conductor.auth.url", task.getReasonForIncompletion());
	}

	@Test
	public void no_conductor_auth_clientId() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost");
		AuthTask authTask = new AuthTask(config);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Missing system property conductor.auth.clientId", task.getReasonForIncompletion());
	}

	@Test
	public void no_conductor_auth_clientSecret() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("deluxe.conductor");
		AuthTask authTask = new AuthTask(config);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Missing system property conductor.auth.clientSecret", task.getReasonForIncompletion());
	}

	@Test
	public void validate_invalid_param() throws Exception {
		Task task = new Task();

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", "it must be map here");

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Invalid 'validate' input parameter. It must be an object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_no_accessToken() throws Exception {
		Task task = new Task();
		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", new HashMap<>());

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("No 'accessToken' parameter provided in 'validate' object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_no_verifyList() throws Exception {
		Task task = new Task();
		Map<String, Object> validate = new HashMap<>();
		validate.put("accessToken", accessToken);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("No 'verifyList' parameter provided in 'validate' object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_empty_accessToken() throws Exception {
		Map<String, Object> validate = new HashMap<>();
		validate.put("accessToken", "");

		Task task = new Task();
		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Parameter 'accessToken' is empty", task.getReasonForIncompletion());
	}

	@Test
	public void validate_wrong_verifyList() throws Exception {
		Map<String, Object> validate = new HashMap<>();
		validate.put("accessToken", accessToken);
		validate.put("verifyList", "wrong type");

		Task task = new Task();
		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Invalid 'verifyList' input parameter. It must be a list", task.getReasonForIncompletion());
	}

	@Test
	public void validate_success() throws Exception {
		Task task = new Task();
		Map<String, Object> validate = new HashMap<>();
		validate.put("accessToken", accessToken);
		validate.put("verifyList", Collections.singletonList(".realm_access.roles | contains([\"uma_authorization\"])"));

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(true, outputData.get("success"));
		assertEquals(null, outputData.get("failedList"));
	}

	@Test
	public void validate_not_success_failed() throws Exception {
		Task task = new Task();
		Map<String, Object> validate = new HashMap<>();
		validate.put("accessToken", accessToken);
		validate.put("verifyList", Collections.singletonList(".dummy.object"));

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		List<Map<String, Object>> failedList = (List<Map<String, Object>>)outputData.get("failedList");
		assertNotNull("No failedList", failedList);
		assertEquals(1, failedList.size());
		Iterator<Map.Entry<String, Object>> iterator = failedList.iterator().next().entrySet().iterator();

		Map.Entry<String, Object> entry1 = iterator.next();
		assertEquals("result", entry1.getKey());
		assertEquals(false, entry1.getValue());

		Map.Entry<String, Object> entry2 = iterator.next();
		assertEquals("condition", entry2.getKey());
		assertEquals(".dummy.object", entry2.getValue());
	}

	@Test
	public void validate_not_success_completed() throws Exception {
		Task task = new Task();
		Map<String, Object> validate = new HashMap<>();
		validate.put("accessToken", accessToken);
		validate.put("verifyList", Collections.singletonList(".dummy.object"));

		Map<String, Object> inputData = task.getInputData();
		inputData.put("failOnError", false);
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		List<Map<String, Object>> failedList = (List<Map<String, Object>>)outputData.get("failedList");
		assertNotNull("No failedList", failedList);
		assertEquals(1, failedList.size());
		Iterator<Map.Entry<String, Object>> iterator = failedList.iterator().next().entrySet().iterator();

		Map.Entry<String, Object> entry1 = iterator.next();
		assertEquals("result", entry1.getKey());
		assertEquals(false, entry1.getValue());

		Map.Entry<String, Object> entry2 = iterator.next();
		assertEquals("condition", entry2.getKey());
		assertEquals(".dummy.object", entry2.getValue());
	}

	@Test
	public void auth_success() throws Exception {
		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(true, outputData.get("success"));
		assertNotNull("No accessToken", outputData.get("accessToken"));
		assertNotNull("No refreshToken", outputData.get("refreshToken"));
	}

	@Test
	public void auth_error_no_data() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/empty");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");
		AuthTask authTask = new AuthTask(config);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("no content", outputData.get("error"));
		assertEquals("server did not return body", outputData.get("errorDescription"));
	}

	@Test
	public void auth_error_failed() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/error");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");
		AuthTask authTask = new AuthTask(config);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("invalid_request", outputData.get("error"));
		assertEquals("Invalid grant_type", outputData.get("errorDescription"));
	}

	@Test
	public void auth_error_completed() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/error");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");
		authTask = new AuthTask(config);

		Task task = new Task();
		task.getInputData().put("failOnError", false);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("invalid_request", outputData.get("error"));
		assertEquals("Invalid grant_type", outputData.get("errorDescription"));
	}

	private static class EchoHandler extends AbstractHandler {

		private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String,Object>>() {};
		
		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
				throws IOException, ServletException {

			if(request.getMethod().equals("POST")) {
				if (request.getRequestURI().equals("/auth/success")) {
					response.addHeader("Content-Type", "application/json; charset=utf-8");
					PrintWriter writer = response.getWriter();
					writer.print(Resources.toString(Resources.getResource("auth-success.json"), Charsets.UTF_8));
					writer.flush();
					writer.close();
				} else if (request.getRequestURI().equals("/auth/error")) {
					String data = Resources.toString(Resources.getResource("auth-error.json"), Charsets.UTF_8);
					response.addHeader("Content-Type", "application/json; charset=utf-8");
					response.addHeader("Content-Length", "" + data.length());
					response.setStatus(400); // Bad request
					PrintWriter writer = response.getWriter();
					writer.print(data);
					writer.flush();
					writer.close();
				} else if (request.getRequestURI().equals("/auth/empty")) {
					response.addHeader("Content-Type", "application/json; charset=utf-8");
					response.addHeader("Content-Length", "0");
					response.setStatus(502); // Bad Gateway
					PrintWriter writer = response.getWriter();
					writer.print("");
					writer.flush();
					writer.close();
				}
			}
		}
	}
}

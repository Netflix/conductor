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
package com.netflix.conductor.contribs.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventProcessor;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.MetadataDAO;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Oleksiy Lysak
 *
 */
public class TestHttpWaitTask {
	private static Server server;

	private static final String ERROR_RESPONSE = "Something went wrong!";
	private static final String TEXT_RESPONSE = "Text Response";
	private static final double NUM_RESPONSE = 42.42d;
	private static String JSON_RESPONSE;
	private static ObjectMapper om = new ObjectMapper();

	private WorkflowExecutor executor = mock(WorkflowExecutor.class);
	private MetadataDAO metadata = mock(MetadataDAO.class);
	private EventProcessor processor = mock(EventProcessor.class);
	private AuthManager authManager = mock(AuthManager.class);

	private Workflow workflow = new Workflow();
	private HttpWaitTask httpTask;

	@BeforeClass
	public static void init() throws Exception {
		server = new Server(7011);
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
	public void setup() {
		Configuration config = mock(Configuration.class);
		when(config.getIntProperty(anyString(), anyInt())).thenReturn(1);
		when(config.getServerId()).thenReturn("test_server_id");
		RestClientManager rcm = new RestClientManager(config);

		httpTask = new HttpWaitTask(config, rcm, new ObjectMapper(), metadata, processor, authManager);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPost() throws Exception {
		Task task = new Task();
		Input input = new Input();
		input.setUri("http://localhost:7011/post");
		Map<String, Object> body = new HashMap<>();
		body.put("input_key1", "value1");
		body.put("input_key2", 45.3d);
		input.setBody(body);
		input.setMethod("POST");
		task.getInputData().put("http_request", input);

		Map<String, Object> event = new HashMap<>();
		event.put("event", "conductor:test");
		event.put("workflowId", ".workflowId");
		event.put("taskId", ".taskId");
		event.put("status", ".status");

		task.getInputData().put("event_wait", event);

		ArgumentCaptor<EventHandler> captor = ArgumentCaptor.forClass(EventHandler.class);
		httpTask.start(workflow, task, executor);

		assertEquals(Task.Status.IN_PROGRESS, task.getStatus());
		Map<String, Object> hr = (Map<String, Object>) task.getOutputData().get("response");
		Object response = hr.get("body");
		assertEquals(Task.Status.IN_PROGRESS, task.getStatus());
		assertTrue("response is: " + response, response instanceof Map);
		Map<String, Object> map = (Map<String, Object>) response;
		Set<String> inputKeys = body.keySet();
		Set<String> responseKeys = map.keySet();
		inputKeys.containsAll(responseKeys);
		responseKeys.containsAll(inputKeys);

		verify(processor, times(1)).refresh();
		verify(metadata, times(1)).addEventHandler(captor.capture());
		assertEquals("conductor:test", captor.getValue().getName());
		assertEquals("true", captor.getValue().getCondition());
		assertTrue(captor.getValue().isActive());
		assertEquals(1, captor.getValue().getActions().size());
		EventHandler.Action action = captor.getValue().getActions().get(0);
		assertEquals(EventHandler.Action.Type.update_task, action.getAction());
		EventHandler.UpdateTask update_task = action.getUpdate_task();
		assertNotNull(update_task);
		assertEquals(".workflowId", update_task.getWorkflowId());
		assertEquals(".taskId", update_task.getTaskId());
		assertEquals(".status", update_task.getStatus());
		assertNull(update_task.getOutput());
		assertNull(update_task.getStatuses());
		assertNull(update_task.getFailedReason());
		assertFalse(update_task.getResetStartTime());
	}

	private static class EchoHandler extends AbstractHandler {

		private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String,Object>>() {};

		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
				throws IOException, ServletException {
			if(request.getMethod().equals("GET") && request.getRequestURI().equals("/text")) {
				PrintWriter writer = response.getWriter();
				writer.print(TEXT_RESPONSE);
				writer.flush();
				writer.close();
			} else if(request.getMethod().equals("GET") && request.getRequestURI().equals("/json")) {
				response.addHeader("Content-Type", "application/json");
				PrintWriter writer = response.getWriter();
				writer.print(JSON_RESPONSE);
				writer.flush();
				writer.close();
			} else if(request.getMethod().equals("GET") && request.getRequestURI().equals("/failure")) {
				response.addHeader("Content-Type", "text/plain");
				response.setStatus(500);
				PrintWriter writer = response.getWriter();
				writer.print(ERROR_RESPONSE);
				writer.flush();
				writer.close();
			} else if(request.getMethod().equals("POST") && request.getRequestURI().equals("/post")) {
				response.addHeader("Content-Type", "application/json");
				BufferedReader reader = request.getReader();
				Map<String, Object> input = om.readValue(reader, mapOfObj);
				Set<String> keys = input.keySet();
				for(String key : keys) {
					input.put(key, key);
				}
				PrintWriter writer = response.getWriter();
				writer.print(om.writeValueAsString(input));
				writer.flush();
				writer.close();
			} else if(request.getMethod().equals("POST") && request.getRequestURI().equals("/post2")) {
				response.addHeader("Content-Type", "application/json");
				response.setStatus(204);
				BufferedReader reader = request.getReader();
				Map<String, Object> input = om.readValue(reader, mapOfObj);
				Set<String> keys = input.keySet();
				System.out.println(keys);
				response.getWriter().close();

			} else if(request.getMethod().equals("GET") && request.getRequestURI().equals("/numeric")) {
				PrintWriter writer = response.getWriter();
				writer.print(NUM_RESPONSE);
				writer.flush();
				writer.close();
			} else if(request.getMethod().equals("POST") && request.getRequestURI().equals("/oauth")) {
				//echo back oauth parameters generated in the Authorization header in the response
				Map<String, String> params = parseOauthParameters(request);
				response.addHeader("Content-Type", "application/json");
				PrintWriter writer = response.getWriter();
				writer.print(om.writeValueAsString(params));
				writer.flush();
				writer.close();
			}
		}

		private Map<String, String> parseOauthParameters(HttpServletRequest request) {
			String paramString = request.getHeader("Authorization").replaceAll("^OAuth (.*)", "$1");
			return Arrays.stream(paramString.split("\\s*,\\s*"))
					.map(pair -> pair.split("="))
					.collect(Collectors.toMap(o -> o[0], o -> o[1].replaceAll("\"","")));
		}
	}
}

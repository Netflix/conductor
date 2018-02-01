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
package com.netflix.conductor.core.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.auth.AuthResponse;
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
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Oleksiy Lysak
 *
 */
@SuppressWarnings("unchecked")
public class TestAuthModule {
	private static Server server;
	private AuthManager authManager;

	@BeforeClass
	public static void init() throws Exception {
		server = new Server(7012);
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
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7012/auth/success");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");
		authManager = new AuthManager(config);
	}

	@Test(expected = IllegalArgumentException.class)
	public void no_conductor_auth_url() throws Exception {
		Configuration config = mock(Configuration.class);

		try {
			new AuthManager(config);
		} catch (IllegalArgumentException ex) {
			assertEquals(AuthManager.MISSING_PROPERTY + AuthManager.PROPERTY_URL, ex.getMessage());
			throw ex;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void no_conductor_auth_clientId() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7012/auth/success");
		try {
			new AuthManager(config);
		} catch (IllegalArgumentException ex) {
			assertEquals(AuthManager.MISSING_PROPERTY + AuthManager.PROPERTY_CLIENT, ex.getMessage());
			throw ex;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void no_conductor_auth_clientSecret() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7012/auth/success");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		try {
			new AuthManager(config);
		} catch (IllegalArgumentException ex) {
			assertEquals(AuthManager.MISSING_PROPERTY + AuthManager.PROPERTY_SECRET, ex.getMessage());
			throw ex;
		}
	}

	@Test
	public void auth_success() throws Exception {
		AuthResponse authResponse = authManager.authorize();
		assertNotNull(authResponse);
		assertNull(authResponse.getError());
		assertNull(authResponse.getErrorDescription());

		assertNotNull("No accessToken", authResponse.getAccessToken());
		assertNotNull("No refreshToken", authResponse.getRefreshToken());
	}

	@Test
	public void auth_error_no_data() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7012/auth/empty");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");

		AuthResponse authResponse = new AuthManager(config).authorize();
		assertNotNull(authResponse);
		assertEquals("no content", authResponse.getError());
		assertEquals("server did not return body", authResponse.getErrorDescription());
	}

	@Test
	public void auth_error_failed() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7012/auth/error");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");

		AuthResponse authResponse = new AuthManager(config).authorize();
		assertNotNull(authResponse);
		assertEquals("invalid_request", authResponse.getError());
		assertEquals("Invalid grant_type", authResponse.getErrorDescription());
	}

	private static class EchoHandler extends AbstractHandler {

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

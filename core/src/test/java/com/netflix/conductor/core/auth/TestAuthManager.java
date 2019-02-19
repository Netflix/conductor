package com.netflix.conductor.core.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.auth.AuthResponse;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAuthManager {
	private static Server server;
	private Configuration config = mock(Configuration.class);
	private Workflow workflow = mock(Workflow.class);
	private ObjectMapper om = new ObjectMapper();

	@BeforeClass
	public static void init() throws Exception {
		server = new Server(7010);
		ServletContextHandler servletContextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
		servletContextHandler.setHandler(new TestAuthManager.EchoHandler());
		server.start();
	}

	@Before
	public void setup() throws Exception {
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/success");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("clientId");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("clientSecret");
	}

	@Test
	public void success() {
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/success");

		AuthManager manager = new AuthManager(config);
		AuthResponse authResponse = null;
		try {
			authResponse = manager.authorize(workflow);
		} catch (Exception e) {
			fail(e.getMessage());
		}
		assertNotNull(authResponse);
		assertNull(authResponse.getError());
		assertNull(authResponse.getErrorDescription());
		assertNotNull(authResponse.getAccessToken());
		assertNotNull(authResponse.getRefreshToken());

		Map<String, Object> accessMap = manager.decode(authResponse.getAccessToken());
		assertNotNull(accessMap);
		assertEquals(1, accessMap.size());
		assertEquals("foo", accessMap.get("access"));

		Map<String, Object> refreshMap = manager.decode(authResponse.getRefreshToken());
		assertNotNull(refreshMap);
		assertEquals(1, refreshMap.size());
		assertEquals("bar", refreshMap.get("refresh"));
	}

	@Test
	public void error() {
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/error");
		AuthManager manager = new AuthManager(config);
		AuthResponse authResponse = null;
		try {
			authResponse = manager.authorize(workflow);
		} catch (Exception e) {
			fail(e.getMessage());
		}
		assertNotNull(authResponse);
		assertNull(authResponse.getAccessToken());
		assertNull(authResponse.getRefreshToken());
		assertEquals("invalid_request", authResponse.getError());
		assertEquals("Invalid grant_type", authResponse.getErrorDescription());
	}

	@Test
	public void no_content() {
		when(config.getProperty("conductor.auth.url", null)).thenReturn("http://localhost:7010/auth/empty");
		AuthManager manager = new AuthManager(config);
		AuthResponse authResponse = null;
		try {
			authResponse = manager.authorize(workflow);
		} catch (Exception e) {
			fail(e.getMessage());
		}
		assertNotNull(authResponse);
		assertNull(authResponse.getAccessToken());
		assertNull(authResponse.getRefreshToken());
		assertEquals("no content", authResponse.getError());
		assertEquals("server did not return body", authResponse.getErrorDescription());
	}

	@Test
	public void validate() {
		String token = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() + 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());
		AuthManager manager = new AuthManager(config);
		Map<String, String> rules = new HashMap<>();
		rules.put("success", ".access == \"foo\"");
		rules.put("wrong", ".access == \"bar\"");
		Map<String, Object> validate = manager.validate(token, rules);
		assertNotNull(validate);
		assertEquals(1, validate.size());
		assertEquals(false, validate.get("wrong"));
	}

	@Test
	public void no_exp() {
		String token = JWT.create()
				.withClaim("access", "foo").sign(Algorithm.none());
		AuthManager manager = new AuthManager(config);
		Map<String, String> rules = new HashMap<>();
		rules.put("success", ".access == \"foo\"");
		rules.put("wrong", ".access == \"bar\"");
		try {
			manager.validate(token, rules);
		} catch (Exception ex) {
			assertEquals("Invalid token. No expiration claim present", ex.getMessage());
		}
	}

	@Test
	public void expired() {
		String token = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() - 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());
		AuthManager manager = new AuthManager(config);
		Map<String, String> rules = new HashMap<>();
		rules.put("success", ".access == \"foo\"");
		rules.put("wrong", ".access == \"bar\"");
		try {
			manager.validate(token, rules);
		} catch (Exception ex) {
			assertEquals("Invalid token. Token is expired", ex.getMessage());
		}
	}

	private static class EchoHandler extends AbstractHandler {
		private ObjectMapper om = new ObjectMapper();

		private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String,Object>>() {};

		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
				throws IOException {

			if(request.getMethod().equals("POST")) {
				if (request.getRequestURI().equals("/auth/success")) {
					String accessToken = JWT.create()
							.withClaim("access", "foo")
							.sign(Algorithm.none());

					String refreshToken = JWT.create()
							.withClaim("refresh", "bar")
							.sign(Algorithm.none());

					AuthResponse auth = new AuthResponse();
					auth.setAccessToken(accessToken);
					auth.setRefreshToken(refreshToken);
					String data = om.writeValueAsString(auth);

					response.addHeader("Content-Type", "application/json; charset=utf-8");
					response.addHeader("Content-Length", "" + data.length());
					PrintWriter writer = response.getWriter();
					writer.print(data);
					writer.flush();
					writer.close();
				} else if (request.getRequestURI().equals("/auth/error")) {
					AuthResponse auth = new AuthResponse();
					auth.setError("invalid_request");
					auth.setErrorDescription("Invalid grant_type");
					String data = om.writeValueAsString(auth);

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

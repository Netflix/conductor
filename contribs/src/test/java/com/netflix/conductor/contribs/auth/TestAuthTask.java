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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.auth.AuthResponse;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
	private static ObjectMapper om = new ObjectMapper();

	@Test
	public void validate_invalid_param() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		AuthTask authTask = new AuthTask(manger);

		Task task = new Task();

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", "it must be map here");

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Invalid 'validate' input parameter. It must be an object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_no_token() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		AuthTask authTask = new AuthTask(manger);

		Task task = new Task();
		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", new HashMap<>());

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("No 'token' parameter provided in 'validate' object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_no_rules() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create().withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();
		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("No 'rules' parameter provided in 'validate' object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_no_exp() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		when(manger.validate(anyString(), any())).thenThrow(new IllegalArgumentException("Invalid token. No expiration claim present"));
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create().withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("access",".access == \"foo\"");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);
		inputData.put("failOnError", false);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(true, outputData.get("success"));
		assertEquals("Invalid token. No expiration claim present", outputData.get("reason"));
		assertEquals(null, outputData.get("failed"));
	}

	@Test
	public void validate_no_exp_fail() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		when(manger.validate(anyString(), any())).thenThrow(new IllegalArgumentException("Invalid token. No expiration claim present"));
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create().withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("access",".access == \"foo\"");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("Invalid token. No expiration claim present", outputData.get("reason"));
		assertEquals("Invalid token. No expiration claim present", task.getReasonForIncompletion());
		assertEquals(null, outputData.get("failed"));
	}

	@Test
	public void validate_expired() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		when(manger.validate(anyString(), any())).thenThrow(new IllegalArgumentException("Invalid token. Token is expired"));
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() - 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("access",".access == \"foo\"");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);
		inputData.put("failOnError", false);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(true, outputData.get("success"));
		assertEquals("Invalid token. Token is expired", outputData.get("reason"));
		assertEquals(null, outputData.get("failed"));
	}

	@Test
	public void validate_expired_fail() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		when(manger.validate(anyString(), any())).thenThrow(new IllegalArgumentException("Invalid token. Token is expired"));
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() - 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("access",".access == \"foo\"");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("Invalid token. Token is expired", outputData.get("reason"));
		assertEquals("Invalid token. Token is expired", task.getReasonForIncompletion());
		assertEquals(null, outputData.get("failed"));
	}

	@Test
	public void validate_empty_accessToken() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		AuthTask authTask = new AuthTask(manger);

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", "");

		Task task = new Task();
		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Parameter 'token' is empty", task.getReasonForIncompletion());
	}

	@Test
	public void validate_wrong_rules() throws Exception {
		AuthManager manger = mock(AuthManager.class);
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create().withClaim("access", "foo").sign(Algorithm.none());

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", "wrong type");

		Task task = new Task();
		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());
		assertEquals("Invalid 'rules' input parameter. It must be an object", task.getReasonForIncompletion());
	}

	@Test
	public void validate_success() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("dummy");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("dummy");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("dummy");
		AuthManager manger = new AuthManager(config);
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() + 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("access",".access == \"foo\"");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(true, outputData.get("success"));
		assertEquals(null, outputData.get("failed"));
	}

	@Test
	public void validate_failed() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("dummy");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("dummy");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("dummy");
		AuthManager manger = new AuthManager(config);
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() + 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("dummy_rule",".dummy.object");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));

		Map<String, Object> failed = (Map<String, Object>)outputData.get("failed");
		assertNotNull(failed);
		assertEquals(1, failed.size());

		Map.Entry<String, Object> entry = failed.entrySet().iterator().next();
		assertEquals("dummy_rule", entry.getKey());
		assertEquals(false, entry.getValue());
	}

	@Test
	public void validate_completed() throws Exception {
		Configuration config = mock(Configuration.class);
		when(config.getProperty("conductor.auth.url", null)).thenReturn("dummy");
		when(config.getProperty("conductor.auth.clientId", null)).thenReturn("dummy");
		when(config.getProperty("conductor.auth.clientSecret", null)).thenReturn("dummy");
		AuthManager manger = new AuthManager(config);
		AuthTask authTask = new AuthTask(manger);
		String accessToken = JWT.create()
				.withClaim("exp", new Date(System.currentTimeMillis() + 60_000))
				.withClaim("access", "foo").sign(Algorithm.none());

		Task task = new Task();

		Map<String, Object> rules = new HashMap<>();
		rules.put("dummy_rule",".dummy.object");

		Map<String, Object> validate = new HashMap<>();
		validate.put("token", accessToken);
		validate.put("rules", rules);

		Map<String, Object> inputData = task.getInputData();
		inputData.put("failOnError", false);
		inputData.put("validate", validate);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));

		Map<String, Object> failed = (Map<String, Object>)outputData.get("failed");
		assertNotNull(failed);
		assertEquals(1, failed.size());

		Map.Entry<String, Object> entry = failed.entrySet().iterator().next();
		assertEquals("dummy_rule", entry.getKey());
		assertEquals(false, entry.getValue());
	}

	@Test
	@Ignore("FIXME: Unhandled (unexpected?) NullPointerException")
	public void auth_success() throws Exception {
		AuthManager manger = mock(AuthManager.class);

		AuthResponse authResponse = new AuthResponse();
		String accessToken = JWT.create()
				.withClaim("access", "foo")
				.sign(Algorithm.none());
		String refreshToken = JWT.create()
				.withClaim("refresh", "bar")
				.sign(Algorithm.none());
		authResponse.setAccessToken(accessToken);
		authResponse.setRefreshToken(refreshToken);
		when(manger.authorize(workflow)).thenReturn(authResponse);

		AuthTask authTask = new AuthTask(manger);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(true, outputData.get("success"));
		assertNotNull("No accessToken", outputData.get("accessToken"));
		assertNotNull("No refreshToken", outputData.get("refreshToken"));
	}

	@Test
	@Ignore("FIXME: Unhandled (unexpected?) NullPointerException")
	public void auth_error_failed() throws Exception {
		AuthManager manger = mock(AuthManager.class);

		AuthResponse authResponse = new AuthResponse();
		authResponse.setError("invalid_request");
		authResponse.setErrorDescription("Invalid grant_type");
		when(manger.authorize(workflow)).thenReturn(authResponse);

		AuthTask authTask = new AuthTask(manger);

		Task task = new Task();
		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.FAILED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("invalid_request", outputData.get("error"));
		assertEquals("Invalid grant_type", outputData.get("errorDescription"));
	}

	@Test
	@Ignore("FIXME: Unhandled (unexpected?) NullPointerException")
	public void auth_error_completed() throws Exception {
		AuthManager manger = mock(AuthManager.class);

		AuthResponse authResponse = new AuthResponse();
		authResponse.setError("invalid_request");
		authResponse.setErrorDescription("Invalid grant_type");
		when(manger.authorize(workflow)).thenReturn(authResponse);

		AuthTask authTask = new AuthTask(manger);

		Task task = new Task();
		task.getInputData().put("failOnError", false);

		authTask.start(workflow, task, executor);
		assertEquals(Task.Status.COMPLETED, task.getStatus());

		Map<String, Object> outputData = task.getOutputData();
		assertEquals(false, outputData.get("success"));
		assertEquals("invalid_request", outputData.get("error"));
		assertEquals("Invalid grant_type", outputData.get("errorDescription"));
	}
}

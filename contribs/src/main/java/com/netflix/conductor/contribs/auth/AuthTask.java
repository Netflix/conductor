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
package com.netflix.conductor.contribs.auth;

import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.auth.AuthResponse;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Map;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
public class AuthTask extends WorkflowSystemTask {
	private static final Logger logger = LoggerFactory.getLogger(AuthTask.class);
	private static final String PARAM_VALIDATE = "validate";
	private static final String PARAM_TOKEN = "token";
	private static final String PARAM_RULES = "rules";
	private AuthManager manger;

	@Inject
	public AuthTask(AuthManager manger) {
		super("AUTH");
		this.manger = manger;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.COMPLETED);

		boolean failOnError = getFailOnError(task);

		// Auth by default if no 'validate' object provided. Otherwise do validation only
		if (task.getInputData().containsKey(PARAM_VALIDATE)) {
			Object object = task.getInputData().get(PARAM_VALIDATE);
			if (!(object instanceof Map)) {
				fail(task, "Invalid '" + PARAM_VALIDATE + "' input parameter. It must be an object");
				return;
			}
			Map<String, Object> map = (Map<String, Object>) object;
			if (!map.containsKey(PARAM_TOKEN)) {
				fail(task, "No '" + PARAM_TOKEN + "' parameter provided in 'validate' object");
				return;
			} else if (StringUtils.isEmpty(map.get(PARAM_TOKEN).toString())) {
				fail(task, "Parameter '" + PARAM_TOKEN + "' is empty");
				return;
			}

			Object rules = map.get(PARAM_RULES);
			if (rules == null) {
				fail(task, "No '" + PARAM_RULES + "' parameter provided in 'validate' object");
				return;
			} else if (!(rules instanceof Map)) {
				fail(task, "Invalid '" + PARAM_RULES + "' input parameter. It must be an object");
				return;
			}

			doValidate(task, failOnError);
		} else {
			doAuth(task, failOnError);
		}
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
	}

	@SuppressWarnings("unchecked")
	private void doValidate(Task task, boolean failOnError) {
		Map<String, Object> validate = (Map<String, Object>) task.getInputData().get(PARAM_VALIDATE);
		String token = (String) validate.get(PARAM_TOKEN);

		Map<String, String> rules = (Map<String, String>) validate.get(PARAM_RULES);

		Map<String, Object> failed = Collections.emptyMap();
		try {
			failed = manger.validate(token, rules);
		} catch (IllegalArgumentException ex) {
			task.getOutputData().put("reason", ex.getMessage());
			if (failOnError) {
				task.getOutputData().put("success", false);
				fail(task, ex.getMessage());
				return;
			}
		}

		task.getOutputData().put("success", failed.isEmpty());
		if (!failed.isEmpty()) {
			task.getOutputData().put("failed", failed);
		}

		// Fail task if any of the conditions failed and failOnError=true
		if (!failed.isEmpty() && failOnError) {
			fail(task, "At least one of the verify conditions failed");
		}
	}

	private void doAuth(Task task, boolean failOnError) throws Exception {
		AuthResponse auth = manger.authorize(task.getCorrelationId());

		if (!StringUtils.isEmpty(auth.getAccessToken())) {
			task.getOutputData().put("success", true);
			task.getOutputData().put("accessToken", auth.getAccessToken());
			task.getOutputData().put("refreshToken", auth.getRefreshToken());
		} else if (!StringUtils.isEmpty(auth.getError())) {
			logger.error("Authorization failed with " + auth.getError() + ":" + auth.getErrorDescription());
			task.getOutputData().put("success", false);
			task.getOutputData().put("error", auth.getError());
			task.getOutputData().put("errorDescription", auth.getErrorDescription());
			if (failOnError) {
				fail(task, auth.getError() + ":" + auth.getErrorDescription());
			}
		}
	}

	private void fail(Task task, String reason) {
		task.setReasonForIncompletion(reason);
		task.setStatus(Status.FAILED);
	}

	private boolean getFailOnError(Task task) {
		Object obj = task.getInputData().get("failOnError");
		if (obj instanceof Boolean) {
			return (boolean) obj;
		} else if (obj instanceof String) {
			return Boolean.parseBoolean((String) obj);
		}
		return true;
	}
}

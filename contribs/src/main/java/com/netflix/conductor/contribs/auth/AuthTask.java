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
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
public class AuthTask extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(AuthTask.class);
    private static final String PARAM_VALIDATE = "validate";
    private static final String PARAM_ACCESS_TOKEN = "accessToken";
    private static final String PARAM_VERIFY_LIST = "verifyList";
    private static final String MISSING_PROPERTY = "Missing system property ";
	private static final String PROPERTY_URL = "conductor.auth.url";
	private static final String PROPERTY_CLIENT = "conductor.auth.clientId";
	private static final String PROPERTY_SECRET = "conductor.auth.clientSecret";
    private final String clientSecret;
	private final String clientId;
    private final String authUrl;

	public AuthTask(Configuration config) {
		super("AUTH");
		authUrl = config.getProperty(PROPERTY_URL, null);
        clientId = config.getProperty(PROPERTY_CLIENT, null);
        clientSecret = config.getProperty(PROPERTY_SECRET, null);
	}
	
	@Override
    @SuppressWarnings("unchecked")
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        task.setStatus(Status.COMPLETED);
	    if (StringUtils.isEmpty(authUrl)) {
            fail(task, MISSING_PROPERTY + PROPERTY_URL);
            return;
        } else if (StringUtils.isEmpty(clientId)) {
            fail(task, MISSING_PROPERTY + PROPERTY_CLIENT);
            return;
        } else if (StringUtils.isEmpty(clientSecret)) {
            fail(task, MISSING_PROPERTY + PROPERTY_SECRET);
            return;
        }

        boolean failOnError = getFailOnError(task);

	    // Auth by default if no 'validate' object provided. Otherwise do validation only
	    if (task.getInputData().containsKey(PARAM_VALIDATE)) {
            Object object = task.getInputData().get(PARAM_VALIDATE);
            if (!(object instanceof Map)) {
                fail(task, "Invalid '" + PARAM_VALIDATE + "' input parameter. It must be an object");
                return;
            }
            Map<String, Object> map = (Map<String, Object>)object;
            if (!map.containsKey(PARAM_ACCESS_TOKEN)) {
                fail(task, "No '" + PARAM_ACCESS_TOKEN + "' parameter provided in 'validate' object");
                return;
            } else if (StringUtils.isEmpty(map.get(PARAM_ACCESS_TOKEN).toString())) {
                fail(task, "Parameter '" + PARAM_ACCESS_TOKEN + "' is empty");
                return;
            }

            if (!map.containsKey(PARAM_VERIFY_LIST)) {
                fail(task, "No '" + PARAM_VERIFY_LIST + "' parameter provided in 'validate' object");
                return;
            } else if (!(map.get(PARAM_VERIFY_LIST) instanceof List)) {
                fail(task, "Invalid '" + PARAM_VERIFY_LIST + "' input parameter. It must be a list");
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
        Map<String, Object> validate = (Map<String, Object>)task.getInputData().get(PARAM_VALIDATE);
        String token = (String)validate.get(PARAM_ACCESS_TOKEN);
        DecodedJWT decoded = JWT.decode(token);
        Map<String, Claim> claims = decoded.getClaims();

        Map<String, Object> payload = new HashMap<>();
        claims.forEach((key, value) -> payload.put(key, value.asMap()));

        ObjectMapper om = new ObjectMapper();
        LoadingCache<String, JsonQuery> queryCache = createQueryCache();

        JsonNode input = om.valueToTree(payload);

        List<String> verifyList = (List)validate.get(PARAM_VERIFY_LIST);
        Map<String, Object> failedList = new HashMap<>();
        verifyList.forEach(condition -> {
            try {
                JsonQuery query = queryCache.get(condition);
                List<JsonNode> result = query.apply(input);
                if (result == null || result.isEmpty()) {
                    logger.error("Verify failed for " + condition  + " with no result!");
                } else {
                    boolean success = Boolean.parseBoolean(result.iterator().next().toString());
                    if (!success) {
                        failedList.put(condition, false);
                    }
                }
            } catch (Exception ex) {
                logger.error("Verify failed for " + condition  + " with " + ex.getMessage(), ex);
                failedList.put(condition, ex.getMessage());
            }
        });

        task.getOutputData().put("success", failedList.isEmpty());
        if (!failedList.isEmpty()) {
            List<Map<Object, Object>> failedObjects = failedList.entrySet().stream().map(entry -> {
                Map<Object, Object> failedItem = new HashMap<>();
                failedItem.put("condition", entry.getKey());
                failedItem.put("result", entry.getValue());
                return failedItem;
            }).collect(toList());
            task.getOutputData().put("failedList", failedObjects);
        }

        // Fail task if any of the conditions failed and failOnError=true
        if (!failedList.isEmpty() && failOnError) {
            fail(task, "At least one of the verify conditions failed");
        }
    }

    private void doAuth(Task task, boolean failOnError) throws Exception {
        AuthResponse auth = authorize();

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
            return (boolean)obj;
        } else if (obj instanceof String) {
            return Boolean.parseBoolean((String)obj);
        }
        return true;
    }

    private AuthResponse authorize() throws Exception {
        Client client = Client.create();
        MultivaluedMap<String, String> data = new MultivaluedMapImpl();
        data.add("grant_type", "client_credentials");
        data.add("client_id", this.clientId);
        data.add("client_secret", this.clientSecret);

        WebResource webResource = client.resource(this.authUrl);

        ClientResponse response = webResource
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .post(ClientResponse.class, data);

        if (response.getStatus() == 200) {
            return response.getEntity(AuthResponse.class);
        } else {
            String entity = response.getEntity(String.class);
            if (StringUtils.isEmpty(entity)) {
                return new AuthResponse("no content", "server did not return body");
            }
            ObjectMapper mapper = new ObjectMapper();
            String reason = response.getStatusInfo().getReasonPhrase();

            // Workaround to handle response like this:
            // Bad request: { ... json here ... }
            if (entity.startsWith(reason)) {
                return mapper.readValue(entity.substring(entity.indexOf(":") + 1), AuthResponse.class);
            } else {
                return mapper.readValue(entity, AuthResponse.class);
            }
        }
    }

    private LoadingCache<String, JsonQuery> createQueryCache() {
        CacheLoader<String, JsonQuery> loader = new CacheLoader<String, JsonQuery>() {
            @Override
            public JsonQuery load(@Nonnull String query) throws JsonQueryException {
                return JsonQuery.compile(query);
            }
        };
        return CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).maximumSize(1000).build(loader);
    }
}

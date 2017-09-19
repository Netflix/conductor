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
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Map;

/**
 * @author Oleksiy Lysak
 *
 */
@Singleton
public class AuthTask extends WorkflowSystemTask {
    private static final Logger logger = LoggerFactory.getLogger(AuthTask.class);
    private static final String MISSING_PROPERTY = "Missing system property ";
	private static final String PARAM_URL = "conductor.auth.url";
	private static final String PARAM_CLIENT = "conductor.auth.clientId";
	private static final String PARAM_SECRET = "conductor.auth.clientSecret";
	private final String authUrl;
	private final String clientId;
	private final String clientSecret;

	public AuthTask(Configuration config) {
		super("AUTH");
		authUrl = config.getProperty(PARAM_URL, null);
        clientId = config.getProperty(PARAM_CLIENT, null);
        clientSecret = config.getProperty(PARAM_SECRET, null);
	}
	
	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        task.setStatus(Status.COMPLETED);
	    if (StringUtils.isEmpty(authUrl)) {
            fail(task, MISSING_PROPERTY + PARAM_URL);
            return;
        } else if (StringUtils.isEmpty(clientId)) {
            fail(task, MISSING_PROPERTY + PARAM_CLIENT);
            return;
        } else if (StringUtils.isEmpty(clientSecret)) {
            fail(task, MISSING_PROPERTY + PARAM_SECRET);
            return;
        }
        boolean failOnError = getFailOnError(task);

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

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		return false;
	}
	
	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
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
            String json = response.getEntity(String.class);
            if (StringUtils.isEmpty(json)) {
                return new AuthResponse("no content", "server did not return body");
            }

            // Workaround to handle response like this:
            // Bad request: { ... json here ... }
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json.substring(json.indexOf(":") + 1), AuthResponse.class);
        }
    }

    private void validate(String token) {
        DecodedJWT decoded = JWT.decode(token);
        Map<String, Claim> claims = decoded.getClaims();
        Map<String, Object> realm_access = claims.get("realm_access").asMap();
        System.out.println("realm_access = " + realm_access);

        Map<String, Object> resource_access = claims.get("resource_access").asMap();
        System.out.println("resource_access = " + resource_access);
    }

    public static void main(String[] args) throws Exception {
        String token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzUmVQUmxxaDBSdUxfcUd5c3dSZXRJbjZNZkZWblp4S2NCdU5GaW55QlM4In0.eyJqdGkiOiI2NDI0Nzk3Ny02MWM3LTQ0MzQtYTM3Yi02NjcwN2Y3ODY5YjciLCJleHAiOjE1MDU0NzkzNjgsIm5iZiI6MCwiaWF0IjoxNTA1NDc5MDY4LCJpc3MiOiJodHRwOi8va2V5Y2xvYWsuc2VydmljZS5vd2YtZGV2OjQwOTg0L2F1dGgvcmVhbG1zL2RlbHV4ZSIsImF1ZCI6ImRlbHV4ZS5jb25kdWN0b3IiLCJzdWIiOiJmN2EzNGRjZi1lYjg5LTQwYTgtYWY2Ni1kZTQ4ODFjNTI4OWEiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJkZWx1eGUuY29uZHVjdG9yIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiNmRkNTFmMzktMDcxZC00MjI1LTgzOTUtNmQ5M2Y2ZDcyYjA0IiwiYWNyIjoiMSIsImNsaWVudF9zZXNzaW9uIjoiNTNiZWM2OTktZDA1NC00NmFkLTg2OTMtYWE2NmUwMGM1NWNjIiwiYWxsb3dlZC1vcmlnaW5zIjpbXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwiY2xpZW50SG9zdCI6IjE3Mi4zMS44NS4xNjciLCJjbGllbnRJZCI6ImRlbHV4ZS5jb25kdWN0b3IiLCJuYW1lIjoiIiwicHJlZmVycmVkX3VzZXJuYW1lIjoic2VydmljZS1hY2NvdW50LWRlbHV4ZS5jb25kdWN0b3IiLCJjbGllbnRBZGRyZXNzIjoiMTcyLjMxLjg1LjE2NyIsImVtYWlsIjoic2VydmljZS1hY2NvdW50LWRlbHV4ZS5jb25kdWN0b3JAcGxhY2Vob2xkZXIub3JnIn0.Sv_MzX-D5pwO2W3FIjdtoUcRlkBgvekbU4WzeokGKVIQxoW3o84B87kzYsBBZzHBzHiM_DRCDgA17wJCC-YtJ5mLOCcfZkceF_IX23YU167F3VJwD26jsjVavW5eUp1f5KtNvj-RkBE2VmAxb21s5_XY6fgXpS3RHy8pJ-o_4uvlH7aDVHuQj8YLgn1g7Pkjc7egiZJ87eidpy3Ukgv7KC7jea0Eu0L0Xw2WLws08YlRry8oztdLOfVWPQr4H6xxLjw-pPVRIbfQU0JtisU45qIQkm3o9uJKZTWvYYZwSOCbvouHuyN4GOvB-hK4McbNHxQrpNJDcj4YQ7OVDWmZkQ";
        DecodedJWT decoded = JWT.decode(token);
        Map<String, Claim> claims = decoded.getClaims();
        Map<String, Object> realm_access = claims.get("realm_access").asMap();
        System.out.println("realm_access = " + realm_access);

        Map<String, Object> resource_access = claims.get("resource_access").asMap();
        System.out.println("resource_access = " + resource_access);
    }

}

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
package com.netflix.conductor.contribs.http;


import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.oauth.client.OAuthClientFilter;
import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.oauth.signature.OAuthSecrets;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Viren
 * Task that enables calling another http endpoint as part of its execution
 */
@Singleton
public class HttpTask extends WorkflowSystemTask {

    public static final String REQUEST_PARAMETER_NAME = "http_request";

    static final String MISSING_REQUEST = "Missing HTTP request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key wiht HttpTask.Input as value. See documentation for HttpTask for required input parameters";

    private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);

    public static final String NAME = "HTTP";

    private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>() {
    };

    private TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>() {
    };

    protected ObjectMapper om = objectMapper();

    protected RestClientManager rcm;

    protected Configuration config;

    private String requestParameter;

    @Inject
    public HttpTask(RestClientManager rcm, Configuration config) {
        this(NAME, rcm, config);
    }

    public HttpTask(String name, RestClientManager rcm, Configuration config) {
        super(name);
        this.rcm = rcm;
        this.config = config;
        this.requestParameter = REQUEST_PARAMETER_NAME;
        logger.info("HttpTask initialized...");
    }

    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        logger.info("--------------0------Starting HTTP TASK ");
        logger.info("--------------0------Starting HTTP TASK NAME =  " + task.getTaskDefName());
        logger.info("--------------0------Starting HTTP Task Workflow ID =  " + workflow.getWorkflowId());
        Object request = task.getInputData().get(requestParameter);
        task.setWorkerId(config.getServerId());
        String url = null;
        Input input = om.convertValue(request, Input.class);
        logger.info("--------------1------URI =" + input.getUri());
        System.out.println("Content type==" + input.getContentType());

        if (request == null) {
            task.setReasonForIncompletion(MISSING_REQUEST);
            task.setStatus(Status.FAILED);
            return;
        } else {
            if (input.getServiceDiscoveryQuery() != null) {
                DNSLookup lookup = new DNSLookup();
                DNSLookup.DNSResponses responses = lookup.lookupService(input.getServiceDiscoveryQuery());
                if (responses != null) {
                    String address = responses.getResponses()[0].address;
                    int port = responses.getResponses()[0].port;
                    url = "http://" + address + ":" + port;
                }
            }
        }

        if (input.getUri() == null) {
            task.setReasonForIncompletion("Missing HTTP URI. See documentation for HttpTask for required input parameters");
            task.setStatus(Status.FAILED);
            return;
        } else {
            if (url != null) {
                input.setUri(url + input.getUri());
                logger.info("--------------2---------- URI = " + input.getUri());
            }
        }
        logger.info("--------------3---------- URI = " + input.getUri());

        if (input.getMethod() == null) {
            task.setReasonForIncompletion("No HTTP method specified");
            task.setStatus(Status.FAILED);
            return;
        }

        HttpResponse httpResponse = callAuth();
        if (httpResponse.body != null) {
            AuthResponse authResponse = (AuthResponse) httpResponse.body;
            if (!StringUtils.isEmpty(authResponse.getAccess_token())) {
                task.getOutputData().put("access_token", authResponse.getAccess_token());
            }
        }
        // validateAuth(httpResponse);

        try {
            logger.info("--------------4---------- URI = " + input.getUri());
            logger.info("--------------4---------- BODY =" + input.getBody());
            HttpResponse response = new HttpResponse();
            if (input.getContentType() != null) {
                if (input.getContentType().equalsIgnoreCase("application/x-www-form-urlencoded")) {
                    String json = new ObjectMapper().writeValueAsString(task.getInputData());
                    JSONObject obj = new JSONObject(json);
                    JSONObject getSth = obj.getJSONObject("http_request");

                    Object main_body = getSth.get("body");
                    String body = main_body.toString();

                    System.out.println("body=====" + body);
                    response = httpCallUrlEncoded(input, body);
                } else {
                    response = httpCall(input);
                }
            } else {
                response = httpCall(input);
            }

            logger.info("response {}, {}", response.statusCode, response.body);
            if (response.statusCode > 199 && response.statusCode < 300) {
                task.setStatus(Status.COMPLETED);
            } else {
                if (response.body != null) {
                    task.setReasonForIncompletion(response.body.toString());
                } else {
                    task.setReasonForIncompletion("No response from the remote service");
                }
                task.setStatus(Status.FAILED);
            }
            if (response != null) {
                task.getOutputData().put("response", response.asMap());
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            task.setStatus(Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
            task.getOutputData().put("response", e.getMessage());
        }
    }

    private HttpResponse callAuth() throws Exception {
        Client client = Client.create();
        MultivaluedMap<String, String> data = new MultivaluedMapImpl();
        data.add("grant_type", "client_credentials");
        data.add("client_id", "deluxe.conductor"); //TODO Read from configuration ?
        data.add("client_secret", "4ecafd6a-a3ce-45dd-bf05-85f2941413d3"); //TODO Read from configuration

        //TODO Read URI from configuration or service discovery
        WebResource webResource = client.resource("https://auth.dmlib.de/api/v1/auth/token?tenant=deluxe");
        ClientResponse clientResponse = webResource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .post(ClientResponse.class, data);

        HttpResponse httpResponse = new HttpResponse();
        httpResponse.statusCode = clientResponse.getStatus();
        httpResponse.headers = clientResponse.getHeaders();
        httpResponse.body = clientResponse.getEntity(AuthResponse.class);

        return httpResponse;
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

    @SuppressWarnings("unchecked")
    private void validateAuth(HttpResponse httpResponse) {
        AuthResponse authResponse = (AuthResponse) httpResponse.body;
        DecodedJWT decoded = JWT.decode(authResponse.getAccess_token());
        Map<String, Claim> claims = decoded.getClaims();
        Map<String, Object> realm_access = claims.get("realm_access").asMap();
        System.out.println("realm_access = " + realm_access);

        Map<String, Object> resource_access = claims.get("resource_access").asMap();
        System.out.println("resource_access = " + resource_access);
    }

    private HttpResponse httpCallUrlEncoded(Input input, String body) throws Exception {
        System.out.println("inside url");

        Client client = Client.create();
        MultivaluedMap formData = new MultivaluedMapImpl();
        Map<String, String> bodyparam = new ObjectMapper().readValue(body, HashMap.class);
        Iterator it = bodyparam.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());

            formData.add(pair.getKey(), pair.getValue());
            it.remove();
        }
        WebResource webResource = client
                .resource(input.uri);


        ClientResponse response = webResource
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .post(ClientResponse.class, formData);

        if (response.getStatus() != 201 && response.getStatus() != 200) {

            throw new RuntimeException("Failed : HTTP error code : "
                    + response.getStatus() + response.getEntity(String.class));


        }
        HttpResponse responsehttp = new HttpResponse();

        responsehttp.body = extractBody(response);

        responsehttp.statusCode = response.getStatus();
        responsehttp.headers = response.getHeaders();
        return responsehttp;

    }

    /**
     * @param input HTTP Request
     * @return Response of the http call
     * @throws Exception If there was an error making http call
     */
    private HttpResponse httpCall(Input input) throws Exception {
        System.out.println("inside normal");

        Client client = rcm.getClient(input);

        if (input.oauthConsumerKey != null) {
            logger.info("Configuring OAuth filter");
            OAuthParameters params = new OAuthParameters().consumerKey(input.oauthConsumerKey).signatureMethod("HMAC-SHA1").version("1.0");
            OAuthSecrets secrets = new OAuthSecrets().consumerSecret(input.oauthConsumerSecret);
            client.addFilter(new OAuthClientFilter(client.getProviders(), params, secrets));
        }

        Builder builder = client.resource(input.uri).type(MediaType.APPLICATION_JSON);

        if (input.body != null) {
            builder.entity(input.body);
        }
        input.headers.entrySet().forEach(e -> {
            builder.header(e.getKey(), e.getValue());
        });

        HttpResponse response = new HttpResponse();
        try {

            ClientResponse cr = builder.accept(input.accept).method(input.method, ClientResponse.class);
            if (cr.getStatus() != 204 && cr.hasEntity()) {
                response.body = extractBody(cr);
            }
            response.statusCode = cr.getStatus();
            response.headers = cr.getHeaders();
            return response;

        } catch (UniformInterfaceException ex) {
            logger.error(ex.getMessage(), ex);
            ClientResponse cr = ex.getResponse();
            logger.error("Status Code: {}", cr.getStatus());
            if (cr.getStatus() > 199 && cr.getStatus() < 300) {

                if (cr.getStatus() != 204 && cr.hasEntity()) {
                    response.body = extractBody(cr);
                }
                response.headers = cr.getHeaders();
                response.statusCode = cr.getStatus();
                return response;

            } else {
                String reason = cr.getEntity(String.class);
                logger.error(reason, ex);
                throw new Exception(reason);
            }
        }

    }

    private Object extractBody(ClientResponse cr) {

        String json = cr.getEntity(String.class);
        logger.info(json);

        try {

            JsonNode node = om.readTree(json);
            if (node.isArray()) {
                return om.convertValue(node, listOfObj);
            } else if (node.isObject()) {
                return om.convertValue(node, mapOfObj);
            } else if (node.isNumber()) {
                return om.convertValue(node, Double.class);
            } else {
                return node.asText();
            }

        } catch (IOException jpe) {
            logger.error(jpe.getMessage(), jpe);
            return json;
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

    @Override
    public boolean isAsync() {
        return true;
    }

    @Override
    public int getRetryTimeInSecond() {
        return 60;
    }

    private static ObjectMapper objectMapper() {
        final ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        om.setSerializationInclusion(Include.NON_NULL);
        om.setSerializationInclusion(Include.NON_EMPTY);
        return om;
    }

    public static class HttpResponse {

        public Object body;

        public MultivaluedMap<String, String> headers;

        public int statusCode;

        @Override
        public String toString() {
            return "HttpResponse [body=" + body + ", headers=" + headers + ", statusCode=" + statusCode + "]";
        }

        public Map<String, Object> asMap() {

            Map<String, Object> map = new HashMap<>();
            map.put("body", body);
            map.put("headers", headers);
            map.put("statusCode", statusCode);

            return map;
        }
    }

    public static class Input {

        private String method;    //PUT, POST, GET, DELETE, OPTIONS, HEAD

        private String vipAddress;

        private Map<String, Object> headers = new HashMap<>();

        private String uri;

        private Object body;

        private String contentType;

        private String accept = MediaType.APPLICATION_JSON;

        private String oauthConsumerKey;

        private String oauthConsumerSecret;

        private String serviceDiscoveryQuery;

        /**
         * @return the method
         */
        public String getMethod() {
            return method;
        }

        /**
         * @param method the method to set
         */
        public void setMethod(String method) {
            this.method = method;
        }

        /**
         * @return the headers
         */
        public Map<String, Object> getHeaders() {
            return headers;
        }

        /**
         * @param headers the headers to set
         */
        public void setHeaders(Map<String, Object> headers) {
            this.headers = headers;
        }

        /**
         * @return the body
         */
        public Object getBody() {
            return body;
        }

        /**
         * @param body the body to set
         */
        public void setBody(Object body) {
            this.body = body;
        }

        /**
         * @return the uri
         */
        public String getUri() {
            return uri;
        }

        /**
         * @param uri the uri to set
         */
        public void setUri(String uri) {
            this.uri = uri;
        }

        /**
         * @return the vipAddress
         */
        public String getVipAddress() {
            return vipAddress;
        }

        /**
         * @param vipAddress the vipAddress to set
         */
        public void setVipAddress(String vipAddress) {
            this.vipAddress = vipAddress;
        }

        /**
         * @return the vipAddress
         */
        public String getContentType() {
            return contentType;
        }

        /**
         * @param vipAddress the vipAddress to set
         */
        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        /**
         * @return the accept
         */
        public String getAccept() {
            return accept;
        }

        /**
         * @param accept the accept to set
         */
        public void setAccept(String accept) {
            this.accept = accept;
        }

        /**
         * @return the OAuth consumer Key
         */
        public String getOauthConsumerKey() {
            return oauthConsumerKey;
        }

        /**
         * @param oauthConsumerKey the OAuth consumer key to set
         */
        public void setOauthConsumerKey(String oauthConsumerKey) {
            this.oauthConsumerKey = oauthConsumerKey;
        }

        /**
         * @return the OAuth consumer secret
         */
        public String getOauthConsumerSecret() {
            return oauthConsumerSecret;
        }

        /**
         * @param oauthConsumerSecret the OAuth consumer secret to set
         */
        public void setOauthConsumerSecret(String oauthConsumerSecret) {
            this.oauthConsumerSecret = oauthConsumerSecret;
        }

        public void setServiceDiscoveryQuery(String query) {
            this.serviceDiscoveryQuery = query;
        }

        public String getServiceDiscoveryQuery() {
            return serviceDiscoveryQuery;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class AuthResponse {
        private String error;
        private String error_description;
        private String access_token;

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public String getError_description() {
            return error_description;
        }

        public void setError_description(String error_description) {
            this.error_description = error_description;
        }

        public String getAccess_token() {
            return access_token;
        }

        public void setAccess_token(String access_token) {
            this.access_token = access_token;
        }
    }
}

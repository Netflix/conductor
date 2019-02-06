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
package com.netflix.conductor.core.execution.batch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.auth.AuthManager;
import com.netflix.conductor.auth.AuthResponse;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.core.DNSLookup;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
@Singleton
public class SherlockBatchProcessor extends AbstractBatchProcessor {
    private static Logger logger = LoggerFactory.getLogger(SherlockBatchProcessor.class);
    private static final TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>() {
    };
    private static final TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>() {
    };
    private ApacheHttpClient4 httpClient = ApacheHttpClient4.create();
    private ExecutorService threadExecutor;
    private WorkflowExecutor workflowExecutor;
    private AuthManager authManager;
    private ObjectMapper mapper;
    private String endpoint;
    private String service;
    private String method;

    @Inject
    public SherlockBatchProcessor(WorkflowExecutor executor, Configuration config,
                                  AuthManager authManager, ObjectMapper mapper) {
        this.authManager = authManager;
        this.workflowExecutor = executor;
        this.mapper = mapper;

        int threadPoolSize = config.getIntProperty("workflow.sweeper.batch.sherlock.worker.count", 100);
        threadExecutor = Executors.newFixedThreadPool(threadPoolSize);

        // The target service endpoint. e.g. /v1/execengine/isworkable
        endpoint = config.getProperty("workflow.sweeper.batch.sherlock.endpoint", "/v1/execengine/isworkable");

        // The target dns service name. e.g. sherlock.service.${TLD}
        service = config.getProperty("workflow.sweeper.batch.sherlock.service", "");

        // The target endpoint method.
        method = config.getProperty("workflow.sweeper.batch.sherlock.method", "POST");
    }

    @Override
    public void run(List<Task> tasks) {
        Map<String, List<Task>> groups = tasks.stream()
                .collect(Collectors.groupingBy(o -> o.getInputData().get("uniqueness").toString()));

        groups.forEach(this::processGroup);
    }

    private void processGroup(String key, List<Task> items) {
        // Get the first from the group
        Task carried = items.get(0);

        Output response = new Output();
        try {
            String uri = DNSLookup.lookup(service);
            if (StringUtils.isEmpty(uri)) {
                throw new IllegalStateException("Service lookup failed for " + service);
            }

            WebResource.Builder builder = httpClient.resource(uri + endpoint).type(MediaType.APPLICATION_JSON);

            // Attaching Deluxe Owf Context
            builder.header("Deluxe-Owf-Context", carried.getCorrelationId());

            // Attach the Authorization header
            Object authorize = carried.getInputData().get("authorize");
            if (Boolean.TRUE.equals(authorize)) {
                AuthResponse auth = authManager.authorize(carried.getCorrelationId());
                builder.header(HttpHeaders.AUTHORIZATION, "Bearer " + auth.getAccessToken());
            }

            builder.entity(carried.getInputData().get("body"));

            // Invoke service
            ClientResponse cr = builder.accept(MediaType.APPLICATION_JSON).method(method, ClientResponse.class);

            // Parse response
            if (cr.getStatus() != 204 && cr.hasEntity()) {
                response.body = extractBody(cr);
            }
            response.statusCode = cr.getStatus();
            response.headers = cr.getHeaders();
        } catch (UniformInterfaceException ex) {
            logger.error("Group {} failed with {}", key, ex.getMessage(), ex);
            ClientResponse cr = ex.getResponse();
            if (cr.getStatus() > 199 && cr.getStatus() < 300) {
                if (cr.getStatus() != 204 && cr.hasEntity()) {
                    response.body = extractBody(cr);
                }
                response.error = ex.getMessage();
                response.headers = cr.getHeaders();
                response.statusCode = cr.getStatus();
            } else {
                String reason = cr.getEntity(String.class);
                logger.error(reason, ex);
            }
        } catch (IllegalStateException ex) {
            response.body = null;
            response.headers = null;
            response.statusCode = -1;
            response.error = ex.getMessage();
        } catch (Exception ex) {
            response.body = null;
            response.headers = null;
            response.statusCode = 0;
            response.error = ex.getMessage();
        }

        List<Future<?>> futures = new ArrayList<>(items.size());
        items.forEach(task -> {
            Future<?> future = threadExecutor.submit(() -> {
                NDC.push("batch-sherlock-" + task.getTaskId());
                try {
                    if (task.getTaskId().equalsIgnoreCase(carried.getTaskId())) {
                        logger.info("batch task execution completed.workflowId=" + task.getWorkflowInstanceId() +
                                ",correlationId=" + task.getCorrelationId() +
                                ",taskId=" + task.getTaskId() +
                                ",taskReferenceName=" + task.getReferenceTaskName() +
                                ",response code=" + response.statusCode + ",response=" + response.body);
                    } else {
                        logger.info("batch task execution completed.carriedTaskId=" + carried.getTaskId() +
                                ",carriedCorrelationId=" + carried.getCorrelationId() +
                                ",workflowId=" + task.getWorkflowInstanceId() +
                                ",correlationId=" + task.getCorrelationId() +
                                ",taskId=" + task.getTaskId() +
                                ",taskReferenceName=" + task.getReferenceTaskName() +
                                ",response code=" + response.statusCode + ",response=" + response.body);
                    }

                    boolean handled = handleStatusMapping(task, response);
                    if (!handled) {
                        if (response.statusCode > 199 && response.statusCode < 300) {
                            task.setStatus(Task.Status.COMPLETED);
                        } else {
                            task.setStatus(Task.Status.FAILED);
                        }
                    }

                    if (task.getStatus() != Task.Status.COMPLETED) {
                        if (response.body != null) {
                            task.setReasonForIncompletion(response.body.toString());
                        } else if (StringUtils.isNotEmpty(response.error)) {
                            task.setReasonForIncompletion(response.error);
                        } else {
                            task.setReasonForIncompletion("No response from the remote service");
                        }
                    }
                    task.getOutputData().put("response", response.asMap());

                    workflowExecutor.updateTask(new TaskResult(task));
                } catch (Exception ex) {
                    logger.error("Update {} failed with {}", task, ex.getMessage(), ex);
                } finally {
                    NDC.remove();
                }
            });
            futures.add(future);
        });

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception ignore) {
            }
        }
    }

    private Object extractBody(ClientResponse cr) {
        String json = cr.getEntity(String.class);
        try {
            JsonNode node = mapper.readTree(json);
            if (node.isArray()) {
                return mapper.convertValue(node, listOfObj);
            } else if (node.isObject()) {
                return mapper.convertValue(node, mapOfObj);
            } else if (node.isNumber()) {
                return mapper.convertValue(node, Double.class);
            } else {
                return node.asText();
            }
        } catch (Exception jpe) {
            logger.error("extractBody failed with " + jpe.getMessage(), jpe);
            return json;
        }
    }

    private boolean handleStatusMapping(Task task, Output response) {
        Object param = task.getInputData().get("status_mapping");
        if (param == null) {
            return false;
        }
        if (!(param instanceof Map)) {
            throw new RuntimeException("The 'status_mapping' is not an object");
        }
        Map<Integer, Task.Status> statusMapping = mapper.convertValue(param, new TypeReference<Map<Integer, Task.Status>>() {
        });
        if (statusMapping.isEmpty()) {
            return false;
        }

        Task.Status taskStatus = statusMapping.get(response.statusCode);
        if (taskStatus == null) {
            return false;
        }

        task.setStatus(taskStatus);
        return true;
    }

    private static class Output {
        public MultivaluedMap<String, String> headers;
        public Object body;
        public String error;
        public int statusCode;

        Map<String, Object> asMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("statusCode", statusCode);
            map.put("headers", headers);
            map.put("error", error);
            map.put("body", body);
            return map;
        }
    }
}

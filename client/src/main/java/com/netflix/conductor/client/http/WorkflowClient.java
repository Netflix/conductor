/*
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
package com.netflix.conductor.client.http;

import com.google.common.base.Preconditions;
import com.netflix.conductor.client.config.ConductorClientConfiguration;
import com.netflix.conductor.client.config.DefaultConductorClientConfiguration;
import com.netflix.conductor.client.exceptions.ConductorClientException;
import com.netflix.conductor.client.telemetry.MetricsContainer;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.sun.jersey.api.client.ClientHandler;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;


/**
 * @author Viren
 */
public class WorkflowClient extends ClientBase {

    private static GenericType<SearchResult<WorkflowSummary>> searchResultWorkflowSummary = new GenericType<SearchResult<WorkflowSummary>>() {
    };

    private static final Logger logger = LoggerFactory.getLogger(WorkflowClient.class);

    /**
     * Creates a default task client
     */
    public WorkflowClient() {
        this(new DefaultClientConfig(), new DefaultConductorClientConfiguration(), null);
    }

    /**
     * @param config REST Client configuration
     */
    public WorkflowClient(ClientConfig config) {
        this(config, new DefaultConductorClientConfiguration(), null);
    }

    /**
     * @param config  REST Client configuration
     * @param handler Jersey client handler.  Useful when plugging in various http client interaction modules (e.g. ribbon)
     */
    public WorkflowClient(ClientConfig config, ClientHandler handler) {
        this(config, new DefaultConductorClientConfiguration(), handler);
    }

    /**
     * @param config  REST Client configuration
     * @param handler Jersey client handler.  Useful when plugging in various http client interaction modules (e.g. ribbon)
     * @param filters Chain of client side filters to be applied per request
     */
    public WorkflowClient(ClientConfig config, ClientHandler handler, ClientFilter... filters) {
        this(config, new DefaultConductorClientConfiguration(), handler, filters);
    }

    /**
     * @param config              REST Client configuration
     * @param clientConfiguration Specific properties configured for the client, see {@link ConductorClientConfiguration}
     * @param handler             Jersey client handler. Useful when plugging in various http client interaction modules (e.g. ribbon)
     * @param filters             Chain of client side filters to be applied per request
     */
    public WorkflowClient(ClientConfig config, ConductorClientConfiguration clientConfiguration, ClientHandler handler, ClientFilter... filters) {
        super(config, clientConfiguration, handler);
        for (ClientFilter filter : filters) {
            super.client.addFilter(filter);
        }
    }

    /**
     * Starts a workflow.
     * If the size of the workflow input payload is bigger than {@link ConductorClientConfiguration#getWorkflowInputPayloadThresholdKB()},
     * it is uploaded to {@link ExternalPayloadStorage}, if enabled, else the workflow is rejected.
     *
     * @param startWorkflowRequest the {@link StartWorkflowRequest} object to start the workflow
     * @return the id of the workflow instance that can be used for tracking
     * @throws ConductorClientException if {@link ExternalPayloadStorage} is disabled or if the payload size is greater than {@link ConductorClientConfiguration#getWorkflowInputMaxPayloadThresholdKB()}
     */
    public String startWorkflow(StartWorkflowRequest startWorkflowRequest) {
        Preconditions.checkNotNull(startWorkflowRequest, "StartWorkflowRequest cannot be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(startWorkflowRequest.getName()), "Workflow name cannot be null or empty");
        Preconditions.checkArgument(StringUtils.isBlank(startWorkflowRequest.getExternalInputPayloadStoragePath()), "External Storage Path must not be set");

        String version = startWorkflowRequest.getVersion() != null ? startWorkflowRequest.getVersion().toString() : "latest";
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            objectMapper.writeValue(byteArrayOutputStream, startWorkflowRequest.getInput());
            byte[] workflowInputBytes = byteArrayOutputStream.toByteArray();
            long workflowInputSize = workflowInputBytes.length;
            MetricsContainer.recordWorkflowInputPayloadSize(startWorkflowRequest.getName(), version, workflowInputSize);
            if (workflowInputSize > conductorClientConfiguration.getWorkflowInputPayloadThresholdKB() * 1024) {
                if (!conductorClientConfiguration.isExternalPayloadStorageEnabled() ||
                        (workflowInputSize > conductorClientConfiguration.getWorkflowInputMaxPayloadThresholdKB() * 1024)) {
                    String errorMsg = String.format("Input payload larger than the allowed threshold of: %d KB", conductorClientConfiguration.getWorkflowInputPayloadThresholdKB());
                    throw new ConductorClientException(errorMsg);
                } else {
                    MetricsContainer
                        .incrementExternalPayloadUsedCount(startWorkflowRequest.getName(), ExternalPayloadStorage.Operation.WRITE.name(), ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT.name());
                    String externalStoragePath = uploadToExternalPayloadStorage(ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT, workflowInputBytes, workflowInputSize);
                    startWorkflowRequest.setExternalInputPayloadStoragePath(externalStoragePath);
                    startWorkflowRequest.setInput(null);
                }
            }
        } catch (IOException e) {
            String errorMsg = String.format("Unable to start workflow:%s, version:%s", startWorkflowRequest.getName(), version);
            logger.error(errorMsg, e);
            MetricsContainer.incrementWorkflowStartErrorCount(startWorkflowRequest.getName(), e);
            throw new ConductorClientException(errorMsg, e);
        }
        try {
            return postForEntity("workflow", startWorkflowRequest, null, String.class, startWorkflowRequest.getName());
        } catch (ConductorClientException e) {
            String errorMsg = String.format("Unable to send start workflow request:%s, version:%s", startWorkflowRequest.getName(), version);
            logger.error(errorMsg, e);
            MetricsContainer.incrementWorkflowStartErrorCount(startWorkflowRequest.getName(), e);
            throw e;
        }
    }

    /**
     * Retrieve a workflow by workflow id
     *
     * @param workflowId   the id of the workflow
     * @param includeTasks specify if the tasks in the workflow need to be returned
     * @return the requested workflow
     */
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        Workflow workflow = getForEntity("workflow/{workflowId}", new Object[]{"includeTasks", includeTasks}, Workflow.class, workflowId);
        populateWorkflowOutput(workflow);
        return workflow;
    }

    /**
     * Retrieve all workflows for a given correlation id and name
     *
     * @param name          the name of the workflow
     * @param correlationId the correlation id
     * @param includeClosed specify if all workflows are to be returned or only running workflows
     * @param includeTasks  specify if the tasks in the workflow need to be returned
     * @return list of workflows for the given correlation id and name
     */
    public List<Workflow> getWorkflows(String name, String correlationId, boolean includeClosed, boolean includeTasks) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(correlationId), "correlationId cannot be blank");

        Object[] params = new Object[]{"includeClosed", includeClosed, "includeTasks", includeTasks};
        List<Workflow> workflows = getForEntity("workflow/{name}/correlated/{correlationId}", params, new GenericType<List<Workflow>>() {
        }, name, correlationId);
        workflows.forEach(this::populateWorkflowOutput);
        return workflows;
    }

    /**
     * Populates the workflow output from external payload storage if the external storage path is specified.
     *
     * @param workflow the workflow for which the output is to be populated.
     */
    private void populateWorkflowOutput(Workflow workflow) {
        if (StringUtils.isNotBlank(workflow.getExternalOutputPayloadStoragePath())) {
            MetricsContainer.incrementExternalPayloadUsedCount(workflow.getWorkflowName(), ExternalPayloadStorage.Operation.READ.name(), ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT.name());
            workflow.setOutput(downloadFromExternalStorage(ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT, workflow.getExternalOutputPayloadStoragePath()));
        }
    }

    /**
     * Removes a workflow from the system
     *
     * @param workflowId      the id of the workflow to be deleted
     * @param archiveWorkflow flag to indicate if the workflow should be archived before deletion
     */
    public void deleteWorkflow(String workflowId, boolean archiveWorkflow) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "Workflow id cannot be blank");

        Object[] params = new Object[]{"archiveWorkflow", archiveWorkflow};
        delete(params, "workflow/{workflowId}/remove", workflowId);
    }

    /**
     * Retrieve all running workflow instances for a given name and version
     *
     * @param workflowName the name of the workflow
     * @param version      the version of the wokflow definition. Defaults to 1.
     * @return the list of running workflow instances
     */
    public List<String> getRunningWorkflow(String workflowName, Integer version) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowName), "Workflow name cannot be blank");
        return getForEntity("workflow/running/{name}", new Object[]{"version", version}, new GenericType<List<String>>() {
        }, workflowName);
    }

    /**
     * Retrieve all workflow instances for a given workflow name between a specific time period
     *
     * @param workflowName the name of the workflow
     * @param version      the version of the workflow definition. Defaults to 1.
     * @param startTime    the start time of the period
     * @param endTime      the end time of the period
     * @return returns a list of workflows created during the specified during the time period
     */
    public List<String> getWorkflowsByTimePeriod(String workflowName, int version, Long startTime, Long endTime) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowName), "Workflow name cannot be blank");
        Preconditions.checkNotNull(startTime, "Start time cannot be null");
        Preconditions.checkNotNull(endTime, "End time cannot be null");

        Object[] params = new Object[]{"version", version, "startTime", startTime, "endTime", endTime};
        return getForEntity("workflow/running/{name}", params, new GenericType<List<String>>() {
        }, workflowName);
    }

    /**
     * Starts the decision task for the given workflow instance
     *
     * @param workflowId the id of the workflow instance
     */
    public void runDecider(String workflowId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        put("workflow/decide/{workflowId}", null, null, workflowId);
    }

    /**
     * Pause a workflow by workflow id
     *
     * @param workflowId the workflow id of the workflow to be paused
     */
    public void pauseWorkflow(String workflowId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        put("workflow/{workflowId}/pause", null, null, workflowId);
    }

    /**
     * Resume a paused workflow by workflow id
     *
     * @param workflowId the workflow id of the paused workflow
     */
    public void resumeWorkflow(String workflowId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        put("workflow/{workflowId}/resume", null, null, workflowId);
    }

    /**
     * Skips a given task from a current RUNNING workflow
     *
     * @param workflowId        the id of the workflow instance
     * @param taskReferenceName the reference name of the task to be skipped
     */
    public void skipTaskFromWorkflow(String workflowId, String taskReferenceName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        Preconditions.checkArgument(StringUtils.isNotBlank(taskReferenceName), "Task reference name cannot be blank");

        put("workflow/{workflowId}/skiptask/{taskReferenceName}", null, workflowId, taskReferenceName);
    }

    /**
     * Reruns the workflow from a specific task
     *
     * @param workflowId           the id of the workflow
     * @param rerunWorkflowRequest the request containing the task to rerun from
     * @return the id of the workflow
     */
    public String rerunWorkflow(String workflowId, RerunWorkflowRequest rerunWorkflowRequest) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        Preconditions.checkNotNull(rerunWorkflowRequest, "RerunWorkflowRequest cannot be null");

        return postForEntity("workflow/{workflowId}/rerun", rerunWorkflowRequest, null, String.class, workflowId);
    }

    /**
     * This API has been marked as deprecated and will be removed in a future release.
     * Please use {@link #restart(String, boolean)} instead.
     */
    @Deprecated
    public void restart(String workflowId) {
        restart(workflowId, false);
    }

    /**
     * Restart a completed workflow
     *
     * @param workflowId           the workflow id of the workflow to be restarted
     * @param useLatestDefinitions if true, use the latest workflow and task definitions when restarting the workflow
     *                             if false, use the workflow and task definitions embedded in the workflow execution when restarting the workflow
     */
    public void restart(String workflowId, boolean useLatestDefinitions) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        Object[] params = new Object[]{"useLatestDefinitions", useLatestDefinitions};
        postForEntity("workflow/{workflowId}/restart", null, params, Void.TYPE, workflowId);
    }

    /**
     * Retries the last failed task in a workflow
     *
     * @param workflowId the workflow id of the workflow with the failed task
     */
    public void retryLastFailedTask(String workflowId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        postForEntityWithUriVariablesOnly("workflow/{workflowId}/retry", workflowId);
    }

    /**
     * Resets the callback times of all IN PROGRESS tasks to 0 for the given workflow
     *
     * @param workflowId the id of the workflow
     */
    public void resetCallbacksForInProgressTasks(String workflowId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        postForEntityWithUriVariablesOnly("workflow/{workflowId}/resetcallbacks", workflowId);
    }

    /**
     * Terminates the execution of the given workflow instance
     *
     * @param workflowId the id of the workflow to be terminated
     * @param reason     the reason to be logged and displayed
     */
    public void terminateWorkflow(String workflowId, String reason) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflow id cannot be blank");
        delete(new Object[]{"reason", reason}, "workflow/{workflowId}", workflowId);
    }

    /**
     * Search for workflows based on payload
     *
     * @param query the search query
     * @return the {@link SearchResult} containing the {@link WorkflowSummary} that match the query
     */
    public SearchResult<WorkflowSummary> search(String query) {
        return getForEntity("workflow/search", new Object[]{"query", query}, searchResultWorkflowSummary);
    }

    /**
     * Paginated search for workflows based on payload
     *
     * @param start    start value of page
     * @param size     number of workflows to be returned
     * @param sort     sort order
     * @param freeText additional free text query
     * @param query    the search query
     * @return the {@link SearchResult} containing the {@link WorkflowSummary} that match the query
     */
    public SearchResult<WorkflowSummary> search(Integer start, Integer size, String sort, String freeText, String query) {
        Object[] params = new Object[]{"start", start, "size", size, "sort", sort, "freeText", freeText, "query", query};
        return getForEntity("workflow/search", params, searchResultWorkflowSummary);
    }
}

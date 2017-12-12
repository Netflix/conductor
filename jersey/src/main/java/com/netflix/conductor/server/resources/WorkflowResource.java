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
package com.netflix.conductor.server.resources;

import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Expose workflow related API operations.
 *
 * @author Viren
 */
@Api(value = "/workflow", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON, tags = "Workflow Management")
@Path("/workflow")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
@Singleton
public class WorkflowResource {

    private WorkflowExecutor executor;

    private ExecutionService service;

    private MetadataService metadata;

    private int maxSearchSize;

    @Inject
    public WorkflowResource(WorkflowExecutor executor, ExecutionService service, MetadataService metadata, Configuration config) {
        this.executor = executor;
        this.service = service;
        this.metadata = metadata;
        this.maxSearchSize = config.getIntProperty("workflow.max.search.size", 5_000);
    }

    @POST
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation("Start a new workflow with StartWorkflowRequest, which allows task to be executed in a domain. Returns workflow ID that can be later used for tracking")
    public String startWorkflow(StartWorkflowRequest request) throws Exception {
        WorkflowDef def = metadata.getWorkflowDef(request.getName(), request.getVersion());
        if (def == null) {
            throw new ApplicationException(Code.NOT_FOUND, "No such workflow found by name=" + request.getName() + ", version=" + request.getVersion());
        }
        return executor.startWorkflow(def.getName(), def.getVersion(), request.getCorrelationId(), request.getInput(), null, request.getTaskToDomain());
    }

    @POST
    @Path("/{name}")
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation("Start a new workflow.  Returns workflow ID that can be later used for tracking.")
    public String startWorkflow(
            @ApiParam(value = "Workflow Name") @PathParam("name") String name,
            @ApiParam(value = "Workflow version to be used") @QueryParam("version") Integer version,
            @ApiParam(value = "Client defined cor-relation id to be used for tracking") @QueryParam("correlationId") String correlationId,
            Map<String, Object> input) throws Exception {

        WorkflowDef def = metadata.getWorkflowDef(name, version);
        if (def == null) {
            throw new ApplicationException(Code.NOT_FOUND, "No such workflow found by name=" + name + ", version=" + version);
        }
        return executor.startWorkflow(def.getName(), def.getVersion(), correlationId, input, null);
    }

    @GET
    @Path("/{name}/correlated/{correlationId}")
    @ApiOperation("Lists workflows for the given correlation id")
    public List<Workflow> getWorkflows(
            @ApiParam(value = "Workflow name") @PathParam("name") String name,
            @ApiParam(value = "Cor-relation ID to be searched for") @PathParam("correlationId") String correlationId,
            @ApiParam(value = "If false, show only currently active workflow.") @QueryParam("includeClosed") @DefaultValue("false") boolean includeClosed, /* TODO:: Need better variable name to indicate the logic it's being used for! */
            @ApiParam(value = "If true, then add task details for the given workflow.") @QueryParam("includeTasks") @DefaultValue("false") boolean includeTasks) throws Exception {
        return service.getWorkflowInstances(name, correlationId, includeClosed, includeTasks);
    }

    @GET
    @Path("/{workflowId}")
    @ApiOperation("Get a workflow by workflow id")
    public Workflow getExecutionStatus(
            @ApiParam(value = "Workflow ID to be searched for.") @PathParam("workflowId") String workflowId,
            @ApiParam(value = "If true, then add task details for the given workflow.") @QueryParam("includeTasks") @DefaultValue("true") boolean includeTasks) throws Exception {
        return service.getExecutionStatus(workflowId, includeTasks);
    }

    @DELETE
    @Path("/{workflowId}/remove")
    @ApiOperation("Remove a workflow by workflow id")
    public void delete(
            @ApiParam(value = "Workflow ID to be removed.") @PathParam("workflowId") String workflowId) throws Exception {
        service.removeWorkflow(workflowId);
    }

    @GET
    @Path("/running/{name}")
    @ApiOperation(value = "Retrieve all the running workflows.", notes = "If either startTime or endTime is not provided, then this API will return all currently running instances of this workflow.")
    public List<String> getRunningWorkflow(
            @ApiParam(value = "Workflow name") @PathParam("name") String workflowName,
            @ApiParam(value = "Workflow version") @QueryParam("version") @DefaultValue("1") Integer version,
            @ApiParam(value = "Search window start time") @QueryParam("startTime") Long startTime,
            @ApiParam(value = "Search window end time") @QueryParam("endTime") Long endTime) throws Exception {
        if (startTime != null && endTime != null) {
            return executor.getWorkflows(workflowName, version, startTime, endTime);
        } else {
            return executor.getRunningWorkflowIds(workflowName);
        }
    }

    @PUT
    @Path("/decide/{workflowId}")
    @ApiOperation(value = "Starts the decision task for a workflow", notes = "This API is deprecated. See '/{workflowId}/decide'.")
    @Deprecated /* API resource here is workflowId.*/
    public void decideOld(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.decide(workflowId);
    }

    @PUT
    @Path("/{workflowId}/decide")
    @ApiOperation("Start the decision task for a workflow")
    public void decide(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.decide(workflowId);
    }


    @PUT
    @Path("/{workflowId}/pause")
    @ApiOperation("Pause a workflow")
    public void pauseWorkflow(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.pauseWorkflow(workflowId);
    }

    @PUT
    @Path("/{workflowId}/resume")
    @ApiOperation("Resume a workflow")
    public void resumeWorkflow(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.resumeWorkflow(workflowId);
    }

    @PUT
    @Path("/{workflowId}/skiptask/{taskReferenceName}")
    @ApiOperation("Skips a given task from a current running workflow")
    public void skipTaskFromWorkflow(
            @ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId,
            @ApiParam(value = "Task reference name to skip") @PathParam("taskReferenceName") String taskReferenceName,
            @ApiParam(value = "Input and output key definitions for task to be skipped") SkipTaskRequest skipTaskRequest) throws Exception {
        executor.skipTaskFromWorkflow(workflowId, taskReferenceName, skipTaskRequest);
    }

    @POST
    @Path("/{workflowId}/rerun")
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @ApiOperation("Reruns the workflow from a specific task")
    public String rerun(
            @ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId,
            @ApiParam(value = "Information required to re-run a workflow instance") RerunWorkflowRequest request) throws Exception {
        request.setReRunFromWorkflowId(workflowId);
        return executor.rerun(request);
    }

    @POST
    @Path("/{workflowId}/restart")
    @ApiOperation("Restarts a completed workflow")
    public void restart(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.rewind(workflowId);
    }

    @POST
    @Path("/{workflowId}/retry")
    @ApiOperation("Retries the last failed task for a given workflow instance")
    public void retry(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.retry(workflowId);
    }

    @POST
    @Path("/{workflowId}/resetcallbacks")
    @ApiOperation("Resets callback times of all in_progress tasks to 0")
    public void reset(@ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId) throws Exception {
        executor.resetCallbacksForInProgressTasks(workflowId);
    }

    @DELETE
    @Path("/{workflowId}")
    @ApiOperation("Terminate workflow execution")
    public void terminate(
            @ApiParam(value = "Workflow ID") @PathParam("workflowId") String workflowId,
            @ApiParam(value = "Reason to terminate workflow execution. For audit.") @QueryParam("reason") String reason) throws Exception {
        executor.terminateWorkflow(workflowId, reason);
    }

    @GET
    @Path("/search")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Search for workflows based on payload and other parameters", notes = "use sort options as sort=<field>:ASC|DESC e.g. sort=name&sort=workflowId:DESC.  If order is not specified, defaults to ASC")
    public SearchResult<WorkflowSummary> search(
            @QueryParam("start") @DefaultValue("0") int start,
            @QueryParam("size") @DefaultValue("100") int size,
            @QueryParam("sort") String sort,
            @QueryParam("freeText") @DefaultValue("*") String freeText,
            @QueryParam("query") String query
    ) {

        if (size > maxSearchSize) {
            throw new ApplicationException(Code.INVALID_INPUT, "Cannot return more than " + maxSearchSize + " workflows.  Please use pagination");
        }
        return service.search(query, freeText, start, size, convert(sort));
    }

    private List<String> convert(String sortStr) {
        List<String> list = new ArrayList<String>();
        if (sortStr != null && sortStr.length() != 0) {
            list = Arrays.asList(sortStr.split("\\|"));
        }
        return list;
    }
}

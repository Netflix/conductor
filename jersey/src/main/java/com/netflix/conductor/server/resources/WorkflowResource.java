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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Error;
import com.netflix.conductor.common.run.*;
import com.netflix.conductor.contribs.correlation.Correlator;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import io.swagger.annotations.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * @author Viren
 */
@Api(value = "/workflow", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON, tags = "Workflow Management")
@Path("/workflow")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON})
@Singleton
public class WorkflowResource {
	private static final Logger logger = LoggerFactory.getLogger(WorkflowResource.class);

	private WorkflowExecutor executor;

	private ExecutionService service;

	private MetadataService metadata;

	private boolean auth_referer_bypass;

	private int maxSearchSize;

	@Inject
	public WorkflowResource(WorkflowExecutor executor, ExecutionService service,
							MetadataService metadata, Configuration config) {
		this.executor = executor;
		this.service = service;
		this.metadata = metadata;
		this.maxSearchSize = config.getIntProperty("workflow.max.search.size", 10_000);
		this.auth_referer_bypass = Boolean.parseBoolean(config.getProperty("workflow.auth.referer.bypass", "false"));
	}

	private String handleCorrelationId(String workflowId, HttpHeaders headers,
									   Response.ResponseBuilder builder) throws JsonProcessingException {
		String correlationId = null;
		if (headers.getRequestHeaders().containsKey(Correlator.headerKey)) {
			Correlator correlator = new Correlator(logger, headers);
			correlator.addIdentifier("urn:deluxe:conductor:workflow:" + workflowId);
			correlator.updateSequenceNo();
			correlationId = correlator.asCorrelationId();
			headers.getRequestHeaders().remove(Correlator.headerKey);
			builder.header(Correlator.headerKey, correlationId);
		}
		return correlationId;
	}

	@POST
	@Produces({MediaType.TEXT_PLAIN})
	@ApiOperation("Start a new workflow with StartWorkflowRequest, which allows task to be executed in a domain")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "WorkflowId", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "AsyncStart", dataType = "boolean", paramType = "header")})
	public Response startWorkflow(StartWorkflowRequest request, @Context HttpHeaders headers) throws Exception {
		WorkflowDef def = metadata.getWorkflowDef(request.getName(), request.getVersion());
		if (def == null) {
			throw new ApplicationException(Code.NOT_FOUND, "No such workflow found by name=" + request.getName() + ", version=" + request.getVersion());
		}
		String workflowId = null;
		if (headers.getRequestHeader(CommonParams.WORKFLOW_ID) != null) {
			workflowId = headers.getRequestHeader(CommonParams.WORKFLOW_ID).get(0);
		}
		// Generate id on this layer as we need to have it before starting workflow
		if (isEmpty(workflowId)) {
			workflowId = IDGenerator.generate();
		}
		Response.ResponseBuilder builder = Response.ok(workflowId);

		String contextToken = null;
		String contextUser = null;
		String traceId = null;
		if (headers.getRequestHeader(CommonParams.AUTH_CONTEXT) != null) {
			contextToken = headers.getRequestHeader(CommonParams.AUTH_CONTEXT).get(0);
			contextUser = executor.validateContextUser(contextToken);
		}

		if (headers.getRequestHeader(CommonParams.PLATFORM_TRACE_ID) != null) {
			traceId = headers.getRequestHeader(CommonParams.PLATFORM_TRACE_ID).get(0);
		}

		boolean asyncStart = false;
		if (headers.getRequestHeader(CommonParams.ASYNC_START) != null) {
			asyncStart = Boolean.parseBoolean(headers.getRequestHeader(CommonParams.ASYNC_START).get(0));
		}
		String correlationId = handleCorrelationId(workflowId, headers, builder);
		if (isNotEmpty(correlationId)) {
			request.setCorrelationId(correlationId);
		}
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			Map<String, Object> auth = executor.validateAuth(def, headers);
			NDC.push("rest-start-" + UUID.randomUUID().toString());
			try {
				logger.info("About to start workflow " + workflowId + ",userInvoked=" + userInvoked + ",path=/{name}");
				executor.startWorkflow(workflowId, def.getName(), def.getVersion(), request.getCorrelationId(),
						request.getInput(), null, request.getTaskToDomain(),
						auth, contextToken, contextUser, traceId, asyncStart, request.getJobPriority());
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-start-" + UUID.randomUUID().toString());
			try {
				logger.info("About to start workflow " + workflowId + ",userInvoked=" + userInvoked + ",path=/{name}");
				executor.startWorkflow(workflowId, def.getName(), def.getVersion(), request.getCorrelationId(),
						request.getInput(), null, request.getTaskToDomain(),
						Collections.emptyMap(), contextToken, contextUser, traceId, asyncStart, request.getJobPriority());
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@POST
	@Path("/{name}")
	@Produces({MediaType.TEXT_PLAIN})
	@ApiOperation("Start a new workflow.  Returns the ID of the workflow instance that can be later used for tracking")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "WorkflowId", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "AsyncStart", dataType = "boolean", paramType = "header")})
	public Response startWorkflow(@Context HttpHeaders headers,
								  @PathParam("name") String name, @QueryParam("version") Integer version, @QueryParam("jobPriority") Integer jobPriority,
								  @QueryParam("correlationId") String correlationId, Map<String, Object> input) throws Exception {

		StartWorkflowRequest request = new StartWorkflowRequest();
		request.setName(name);
		request.setVersion(version);
		request.setCorrelationId(correlationId);
		request.setJobPriority(jobPriority);
		request.setInput(input);

		return startWorkflow(request, headers);
	}

	@GET
	@Path("/{name}/correlated/{correlationId}")
	@ApiOperation("Lists workflows for the given correlation id")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", responseContainer = "List", response = Workflow.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public List<Workflow> getWorkflows(@PathParam("name") String name, @PathParam("correlationId") String correlationId,
									   @QueryParam("includeClosed") @DefaultValue("false") boolean includeClosed,
									   @QueryParam("includeTasks") @DefaultValue("false") boolean includeTasks) throws Exception {
		return service.getWorkflowInstances(name, correlationId, includeClosed, includeTasks);
	}

	@GET
	@Path("/{workflowId}")
	@ApiOperation("Gets the workflow by workflow id")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = Workflow.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Workflow getExecutionStatus(
		@PathParam("workflowId") String workflowId,
		@QueryParam("includeTasks") @DefaultValue("true") boolean includeTasks) throws Exception {
		return service.getExecutionStatus(workflowId, includeTasks);
	}

	@DELETE
	@Path("/{workflowId}/remove")
	@ApiOperation("Removes the workflow from the system")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Response delete(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		NDC.push("rest-remove-" + UUID.randomUUID().toString());
		try {
			executor.removeWorkflowNotImplemented(workflowId);
		} finally {
			NDC.remove();
		}
		return builder.build();
	}

	@GET
	@Path("/running/{name}")
	@ApiOperation("Retrieve all the running workflows")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", responseContainer = "List", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public List<String> getRunningWorkflow(@PathParam("name") String workflowName, @QueryParam("version") @DefaultValue("1") Integer version,
										   @QueryParam("startTime") Long startTime, @QueryParam("endTime") Long endTime) throws Exception {
		if (startTime != null && endTime != null) {
			return executor.getWorkflows(workflowName, version, startTime, endTime);
		} else {
			return executor.getRunningWorkflowIds(workflowName);
		}
	}

	@PUT
	@Path("/decide/{workflowId}")
	@ApiOperation("Starts the decision task for a workflow")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 204, message = "SUCCESS")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public void decide(@PathParam("workflowId") String workflowId) throws Exception {
		NDC.push("rest-decide-" + UUID.randomUUID().toString());
		try {
			executor.decide(workflowId);
		} finally {
			NDC.remove();
		}
	}

	@PUT
	@Path("/{workflowId}/pause")
	@ApiOperation("Pauses the workflow")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 204, message = "SUCCESS")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Response pauseWorkflow(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		String correlationId = handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			executor.validateAuth(workflowId, headers);
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			NDC.push("rest-pause-" + UUID.randomUUID().toString());
			try {
				logger.info("About to pause workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/pause");
				executor.pauseWorkflow(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-pause-" + UUID.randomUUID().toString());
			try {
				logger.info("About to pause workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/pause");
				executor.pauseWorkflow(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@PUT
	@Path("/{workflowId}/resume")
	@ApiOperation("Resumes the workflow")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 204, message = "SUCCESS")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Response resumeWorkflow(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		String correlationId = handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-resume-" + UUID.randomUUID().toString());
			try {
				logger.info("About to resume workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/resume");
				executor.resumeWorkflow(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-resume-" + UUID.randomUUID().toString());
			try {
				logger.info("About to resume workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/resume");
				executor.resumeWorkflow(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@PUT
	@Path("/{workflowId}/skiptask/{taskReferenceName}")
	@ApiOperation("Skips a given task from a current running workflow")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 415, message = "NO_CONTENT")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public void skipTaskFromWorkflow(@PathParam("workflowId") String workflowId, @PathParam("taskReferenceName") String taskReferenceName,
									 SkipTaskRequest skipTaskRequest) throws Exception {
		NDC.push("rest-skipTask-" + UUID.randomUUID().toString());
		try {
			executor.skipTaskFromWorkflow(workflowId, taskReferenceName, skipTaskRequest);
		} finally {
			NDC.remove();
		}
	}

	@POST
	@Path("/{workflowId}/rerun")
	@ApiOperation("Reruns the workflow from a specific task")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response rerun(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId, RerunWorkflowRequest request) throws Exception {
		Response.ResponseBuilder builder = Response.ok(workflowId);
		String correlationId = handleCorrelationId(workflowId, headers, builder);
		request.setReRunFromWorkflowId(workflowId);
		request.setCorrelationId(correlationId);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-rerun-" + UUID.randomUUID().toString());
			try {
				logger.info("About to rerun workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/rerun");
				executor.rerun(request);
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-rerun-" + UUID.randomUUID().toString());
			try {
				logger.info("About to rerun workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/rerun");
				executor.rerun(request);
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@POST
	@Path("/{workflowId}/restart")
	@ApiOperation("Restarts a completed workflow")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 204, message = "SUCCESS")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Response restart(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		String correlationId = handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-restart-" + UUID.randomUUID().toString());
			try {
				logger.info("About to restart workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/restart");
				executor.rewind(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-restart-" + UUID.randomUUID().toString());
			try {
				logger.info("About to restart workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/restart");
				executor.rewind(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		}

		return builder.build();
	}

	@POST
	@Path("/{workflowId}/retry")
	@ApiOperation("Retries the last failed task")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 204, message = "SUCCESS")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Response retry(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		String correlationId = handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-retry-" + UUID.randomUUID().toString());
			try {
				logger.info("About to retry workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/retry");
				executor.retry(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		} else
		{
			NDC.push("rest-retry-" + UUID.randomUUID().toString());
			try {
				logger.info("About to retry workflowId " + workflowId + ",userInvoked=" + userInvoked+",path=/{workflowId}/retry");
				executor.retry(workflowId, correlationId);
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@DELETE
	@Path("/{workflowId}")
	@ApiOperation("Terminate workflow execution")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 204, message = "SUCCESS")})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Consumes(MediaType.WILDCARD)
	public Response terminate(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId, @QueryParam("reason") String reason) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-terminate-" + UUID.randomUUID().toString());
			try {
				logger.info("About to terminate workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}");
				executor.terminateWorkflow(workflowId, StringUtils.defaultIfEmpty(reason, "Terminated from api"));
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-terminate-" + UUID.randomUUID().toString());
			try {
				logger.info("About to terminate workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}");
				executor.terminateWorkflow(workflowId, StringUtils.defaultIfEmpty(reason, "Terminated from api"));
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@POST
	@Path("/terminate")
	@ApiOperation("Terminate multiple workflows execution")
	@ApiResponses(value = {
			@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
			@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
			@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
			@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
			@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
			@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
			@ApiResponse(code = 204, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
			@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response terminateWorkflows(@Context HttpHeaders headers,List<String> workflowIds, @QueryParam("reason") String reason) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		for (int i = 0; i < workflowIds.size(); i++) {
			handleCorrelationId(workflowIds.get(i), headers, builder);
			if (!bypassAuth(headers)) {
				String primarRole = executor.checkUserRoles(headers);
				if (!primarRole.endsWith("admin")) {
					throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
				}
				executor.validateAuth(workflowIds.get(i), headers);
				try {
					executor.terminateWorkflow(workflowIds.get(i), StringUtils.defaultIfEmpty(reason, "Terminated from api"));
				} finally {
					NDC.remove();
				}
			} else {
				try {
					executor.terminateWorkflow(workflowIds.get(i), StringUtils.defaultIfEmpty(reason, "Terminated from api"));
				} finally {
					NDC.remove();
				}
			}
		}
		return builder.build();
	}


	@POST
	@Path("/cancel")
	@ApiOperation("Cancel multiple workflows execution")
	@ApiResponses(value = {
			@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
			@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
			@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
			@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
			@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
			@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
			@ApiResponse(code = 204, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
			@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response cancelWorkflows(@Context HttpHeaders headers,List<String> workflowIds, @QueryParam("reason") String reason) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		for (int i = 0; i < workflowIds.size(); i++) {
			handleCorrelationId(workflowIds.get(i), headers, builder);
			if (!bypassAuth(headers)) {
				String primarRole = executor.checkUserRoles(headers);
				if (!primarRole.endsWith("admin")) {
					throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
				}
				executor.validateAuth(workflowIds.get(i), headers);
				NDC.push("rest-cancel-" + UUID.randomUUID().toString());
				try {
					executor.cancelWorkflow(workflowIds.get(i), StringUtils.defaultIfEmpty(reason, "Cancelled from api"));
				} finally {
					NDC.remove();
				}
			} else {
				NDC.push("rest-cancel-" + UUID.randomUUID().toString());
				try {
					executor.cancelWorkflow(workflowIds.get(i), StringUtils.defaultIfEmpty(reason, "Cancelled from api"));
				} finally {
					NDC.remove();
				}
			}
		}
		return builder.build();
	}

	@POST
	@Path("/complete")
	@ApiOperation("Force complete multiple workflows execution")
	@ApiResponses(value = {
			@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
			@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
			@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
			@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
			@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
			@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
			@ApiResponse(code = 204, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
			@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response completeWorkflows(@Context HttpHeaders headers,List<String> workflowIds, @QueryParam("reason") String reason) throws Exception {
		Response.ResponseBuilder builder = Response.noContent();
		for (int i = 0; i < workflowIds.size(); i++) {
			handleCorrelationId(workflowIds.get(i), headers, builder);
			if (!bypassAuth(headers)) {
				String primarRole = executor.checkUserRoles(headers);
				if (!primarRole.endsWith("admin")) {
					throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
				}
				executor.validateAuth(workflowIds.get(i), headers);
				NDC.push("rest-complete-" + UUID.randomUUID().toString());
				try {
					executor.forceCompleteWorkflow(workflowIds.get(i), StringUtils.defaultIfEmpty(reason, "Force completed by API"));
				} finally {
					NDC.remove();
				}
			} else {
				NDC.push("rest-complete-" + UUID.randomUUID().toString());
				try {
					executor.forceCompleteWorkflow(workflowIds.get(i), StringUtils.defaultIfEmpty(reason, "Force completed by API"));
				} finally {
					NDC.remove();
				}
			}

		}
		return builder.build();
	}


	@POST
	@Path("/{workflowId}/cancel")
	@ApiOperation("Cancel workflow execution")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response cancel(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId, @QueryParam("reason") String reason) throws Exception {
		Response.ResponseBuilder builder = Response.ok(workflowId);
		handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-cancel-" + UUID.randomUUID().toString());
			try {

				logger.info("About to cancel workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/cancel");

				executor.cancelWorkflow(workflowId, StringUtils.defaultIfEmpty(reason, "Cancelled from api"));
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-cancel-" + UUID.randomUUID().toString());
			try {
				logger.info("About to cancel workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/cancel");
				executor.cancelWorkflow(workflowId, StringUtils.defaultIfEmpty(reason, "Cancelled from api"));
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@POST
	@Path("/{workflowId}/complete")
	@ApiOperation("Force complete workflow execution")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", response = String.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@Produces(MediaType.TEXT_PLAIN)
	public Response complete(@Context HttpHeaders headers, @PathParam("workflowId") String workflowId) throws Exception {
		Response.ResponseBuilder builder = Response.ok(workflowId);
		handleCorrelationId(workflowId, headers, builder);
		String userInvoked = executor.decodeAuthorizationUser(headers);
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			executor.validateAuth(workflowId, headers);
			NDC.push("rest-complete-" + UUID.randomUUID().toString());
			try {
				logger.info("About to complete workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/complete");
				executor.forceCompleteWorkflow(workflowId, "Force completed by API");
			} finally {
				NDC.remove();
			}
		} else {
			NDC.push("rest-complete-" + UUID.randomUUID().toString());
			try {
				logger.info("About to complete workflowId " + workflowId + ",userInvoked=" + userInvoked + ",path=/{workflowId}/complete");
				executor.forceCompleteWorkflow(workflowId, "Force completed by API");
			} finally {
				NDC.remove();
			}
		}
		return builder.build();
	}

	@ApiOperation(value = "Search for workflows based in payload and other parameters", notes = "use sort options as sort=<field>:ASC|DESC e.g. sort=name&sort=workflowId:DESC.  If order is not specified, defaults to ASC")
	@ApiResponses(value = {
		@ApiResponse(code = 404, message = "NOT_FOUND", response = Error.class),
		@ApiResponse(code = 400, message = "INVALID_INPUT", response = Error.class),
		@ApiResponse(code = 409, message = "CONFLICT", response = Error.class),
		@ApiResponse(code = 500, message = "INTERNAL_ERROR", response = Error.class),
		@ApiResponse(code = 401, message = "UNAUTHORIZED", response = Error.class),
		@ApiResponse(code = 501, message = "NOT_IMPLEMENTED", response = Error.class),
		@ApiResponse(code = 200, message = "SUCCESS", responseContainer = "List", response = WorkflowSummary.class)})
	@ApiImplicitParams({@ApiImplicitParam(name = "Deluxe-Owf-Context", dataType = "string", paramType = "header"),
		@ApiImplicitParam(name = "Platform-Trace-Id", dataType = "string", paramType = "header")})
	@GET
	@Consumes(MediaType.WILDCARD)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/search")
	public SearchResult<WorkflowSummary> search(
		@QueryParam("start") @DefaultValue("0") int start,
		@QueryParam("size") @DefaultValue("100") int size,
		@QueryParam("sort") String sort,
		@QueryParam("freeText") @DefaultValue("*") String freeText,
		@QueryParam("query") String query,
		@QueryParam("from") String from,
		@QueryParam("end") String end
	) {

		if (size > maxSearchSize) {
			throw new ApplicationException(Code.INVALID_INPUT, "Cannot return more than " + maxSearchSize + " workflows.  Please use pagination");
		}
		return service.search(query, freeText, start, size, convert(sort), from, end);
	}

	@POST
	@Path("/errorRegistrySearch")
	@ApiOperation("Search error registry")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public List<WorkflowError>  searchErrorRegistry(WorkflowErrorRegistry workflowErrorRegistry,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			return executor.searchErrorRegistry(workflowErrorRegistry);
		} else {
			return executor.searchErrorRegistry(workflowErrorRegistry);
		}
	}

	@POST
	@Path("/errorRegistryList")
	@ApiOperation("Get error registry list")
	@Produces(MediaType.APPLICATION_JSON)
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public List<WorkflowErrorRegistry>  searchErrorRegistryList(WorkflowErrorRegistry workflowErrorRegistry,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			return executor.searchErrorRegistryList(workflowErrorRegistry);
		} else {
			return executor.searchErrorRegistryList(workflowErrorRegistry);
		}
	}


	private List<String> convert(String sortStr) {
		List<String> list = new ArrayList<String>();
		if (sortStr != null && sortStr.length() != 0) {
			list = Arrays.asList(sortStr.split("\\|"));
		}
		return list;
	}

	private boolean bypassAuth(HttpHeaders headers) {
		if (!auth_referer_bypass)
			return false;

		List<String> strings = headers.getRequestHeader("Referer");
		if (CollectionUtils.isEmpty(strings))
			return false;
		return strings.get(0).contains("/docs");
	}


}

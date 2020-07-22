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
package com.netflix.conductor.server.resources;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.service.MetadataService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import io.swagger.annotations.*;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.collections.CollectionUtils;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

/**
 * @author Viren
 *
 */
@Api(value="/metadata", produces=MediaType.APPLICATION_JSON, consumes=MediaType.APPLICATION_JSON, tags="Metadata Management")
@Path("/metadata")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON})
public class MetadataResource {

	private MetadataService service;
	private boolean auth_referer_bypass;
	private WorkflowExecutor executor;
	@Inject
	public MetadataResource(MetadataService service,Configuration config,WorkflowExecutor executor) {
		this.auth_referer_bypass = Boolean.parseBoolean(config.getProperty("workflow.auth.referer.bypass", "false"));
		this.service = service;
		this.executor = executor;
	}

	@POST
	@Path("/workflow")
	@ApiOperation("Create a new workflow definition")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void create(WorkflowDef def,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			service.registerWorkflowDef(def);
		} else {
			service.registerWorkflowDef(def);
		}
	}
	
	@PUT
	@Path("/workflow")
	@ApiOperation("Create or update workflow definition")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void update(List<WorkflowDef> defs,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			service.updateWorkflowDef(defs);
		} else {
			service.updateWorkflowDef(defs);
		}
	}

	@GET
	@ApiOperation("Retrieves workflow definition along with blueprint")
	@Path("/workflow/{name}")
	public WorkflowDef get(@PathParam("name") String name, @QueryParam("version") Integer version) throws Exception {
		return service.getWorkflowDef(name, version);
	}

	@GET
	@ApiOperation("Retrieves all workflow definition along with blueprint")
	@Path("/workflow")
	public List<WorkflowDef> getAll() throws Exception {
		return service.getWorkflowDefs();
	}
	
	@POST
	@Path("/taskdefs")
	@ApiOperation("Create new task definition(s)")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void registerTaskDef(List<TaskDef> taskDefs,@Context HttpHeaders headers) throws Exception {
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			service.registerTaskDef(taskDefs);
		} else {
			service.registerTaskDef(taskDefs);
		}
	}
	
	@PUT
	@Path("/taskdefs")
	@ApiOperation("Update an existing task")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void registerTaskDef(TaskDef taskDef,@Context HttpHeaders headers) throws Exception {
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			service.updateTaskDef(taskDef);
		} else {
			service.updateTaskDef(taskDef);
		}
	}

	@GET
	@Path("/taskdefs")
	@ApiOperation("Gets all task definition")
	@Consumes({MediaType.WILDCARD})
	public List<TaskDef> getTaskDefs() throws Exception{
		return service.getTaskDefs();
	}
	
	@GET
	@Path("/taskdefs/{tasktype}")
	@ApiOperation("Gets the task definition")
	@Consumes({MediaType.WILDCARD})
	public TaskDef getTaskDef(@PathParam("tasktype") String taskType) throws Exception {
		return service.getTaskDef(taskType);
	}
	
	@DELETE
	@Path("/taskdefs/{tasktype}")
	@ApiOperation("Remove a task definition")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void unregisterTaskDef(@PathParam("tasktype") String taskType,@Context HttpHeaders headers){
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			service.unregisterTaskDef(taskType);
		} else {
			service.unregisterTaskDef(taskType);
		}
	}

	@DELETE
	@Path("/workflow/{name}")
	@ApiOperation("Remove a workflow definition")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void unregisterWorkflow(@PathParam("name") String name, @QueryParam("version") Integer version,@Context HttpHeaders headers){
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (!primarRole.endsWith("admin")) {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
			service.unregisterWorkflow(name, version);
		} else {
			service.unregisterWorkflow(name, version);
		}
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

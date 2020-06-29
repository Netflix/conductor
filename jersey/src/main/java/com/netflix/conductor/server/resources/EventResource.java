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

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.core.events.EventProcessor;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.service.MetadataService;

import io.swagger.annotations.*;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.core.config.Configuration;
import org.apache.commons.collections.CollectionUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;


/**
 * @author Viren
 *
 */
@Api(value="/event", produces=MediaType.APPLICATION_JSON, consumes=MediaType.APPLICATION_JSON, tags="Event Services")
@Path("/event")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON})
@Singleton
public class EventResource {

	private MetadataService service;
	
	private EventProcessor ep;

	private boolean auth_referer_bypass;

	private WorkflowExecutor executor;
	@Inject
	public EventResource(MetadataService service, EventProcessor ep,Configuration config,WorkflowExecutor executor) {
		this.service = service;
		this.ep = ep;
		this.executor = executor;
		this.auth_referer_bypass = Boolean.parseBoolean(config.getProperty("workflow.auth.referer.bypass", "false"));
	}

	@POST
	@ApiOperation("Add a new event handler.")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void addEventHandler(EventHandler eventHandler,@Context HttpHeaders headers) throws Exception {
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (primarRole.contains("admin")) {
				service.addEventHandler(eventHandler);
			} else {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
		} else {
			service.addEventHandler(eventHandler);
		}
	}

	@PUT
	@ApiOperation("Update an existing event handler.")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void updateEventHandler(EventHandler eventHandler,@Context HttpHeaders headers) throws Exception {
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (primarRole.contains("admin")) {
				service.updateEventHandler(eventHandler);
			} else {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
		} else {
			service.updateEventHandler(eventHandler);
		}
	}

	@PUT
	@Path("/{name}/disable")
	@ApiOperation("Disable an event handler")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void disable(@PathParam("name") String name,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (primarRole.contains("admin")) {
				service.disableEventHandler(name);
			} else {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
		} else {
			service.disableEventHandler(name);
		}
	}

	@PUT
	@Path("/{name}/enable")
	@ApiOperation("Enable an event handler")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void enable(@PathParam("name") String name,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (primarRole.contains("admin")) {
				service.enableEventHandler(name);
			} else {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
		} else {
			service.enableEventHandler(name);
		}
	}

	@POST
	@Path("/refresh")
	@ApiOperation("Force conductor to refresh event handlers")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void refresh(@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (primarRole.contains("admin")) {
				ep.refresh();
			} else {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
		} else {
			ep.refresh();
		}
	}

	@DELETE
	@Path("/{name}")
	@ApiOperation("Remove an event handler")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "Authorization", dataType = "string", paramType = "header")})
	public void removeEventHandlerStatus(@PathParam("name") String name,@Context HttpHeaders headers) throws Exception{
		if (!bypassAuth(headers)) {
			String primarRole = executor.checkUserRoles(headers);
			if (primarRole.contains("admin")) {
				service.removeEventHandlerStatus(name);
			} else {
				throw new ApplicationException(Code.UNAUTHORIZED, "User does not have access privileges");
			}
		} else {
			service.removeEventHandlerStatus(name);
		}
	}

	@GET
	@ApiOperation("Get all the event handlers")
	public List<EventHandler> getEventHandlers() {
		return service.getEventHandlers();
	}
	
	@GET
	@Path("/{event}")
	@ApiOperation("Get event handlers for a given event")
	public List<EventHandler> getEventHandlersForEvent(@PathParam("event") String event, @QueryParam("activeOnly") @DefaultValue("true") boolean activeOnly) {
		return service.getEventHandlersForEvent(event, activeOnly);
	}
	
	@GET
	@Path("/queues")
	@ApiOperation("Get registered queues")
	public Map<String, ?> getEventQueues(@QueryParam("verbose") @DefaultValue("false") boolean verbose) {
		return (verbose ? ep.getQueueSizes() : ep.getQueues());
	}

	@GET
	@Path("/queues/providers")
	@ApiOperation("Get registered queue providers")
	public List<String> getEventQueueProviders() {
		return EventQueues.providers();
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

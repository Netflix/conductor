package com.netflix.conductor.server.resources;

import com.google.common.base.Preconditions;
import com.netflix.conductor.common.metadata.tasks.CompleteTaskResult;
import com.netflix.conductor.service.FloService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Api(value = "/flo", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON, tags = "Flo APIs")
@Path("/flo")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON})
@Singleton
public class FloResource {

    private final FloService floService;

    @Inject
    public FloResource(FloService floService) {
        this.floService = floService;
    }

    //TODO: Replace with PUT once Reminder service supports PUT/Perf is done, whichever is earlier
    //    @PUT
    //    @Path("/{workflowId}/completewaitstatetask/{instanceState}/{unblockedBy}")
    @GET
    @Path("/completewaitstatetask")
    @ApiOperation("Completes a WAIT task from given running workflow")
    @Consumes(MediaType.WILDCARD)
    public CompleteTaskResult completeWaitStateTask(@QueryParam("workflowId") String workflowId,
                                                    @QueryParam("instanceState") String instanceState,
                                                    @QueryParam("unblockedBy") String unblockedBy) {
        Preconditions.checkArgument(StringUtils.isNotBlank(workflowId), "workflowId cannot be null or empty.");
        Preconditions.checkArgument(StringUtils.isNotBlank(instanceState), "instanceState cannot be null or empty.");
        Preconditions.checkArgument(StringUtils.isNotBlank(unblockedBy), "unblockedBy cannot be null or empty.");

        return floService.completeWaitStateTask(workflowId, instanceState, unblockedBy);
    }
}

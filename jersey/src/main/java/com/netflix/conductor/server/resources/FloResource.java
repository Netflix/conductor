package com.netflix.conductor.server.resources;

import com.google.common.base.Preconditions;
import com.netflix.conductor.common.metadata.tasks.CompleteTaskResult;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.service.FloService;
import com.netflix.conductor.service.WorkflowService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Api(value = "/flo", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON, tags = "Flo APIs")
@Path("/flo")
@Produces({MediaType.APPLICATION_JSON})
@Consumes({MediaType.APPLICATION_JSON})
@Singleton
public class FloResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(FloResource.class);

    public static final String TRACING_ID = "tracingId";
    private final FloService floService;

    private final WorkflowService workflowService;

    @Inject
    public FloResource(FloService floService, WorkflowService workflowService) {
        this.floService = floService;
        this.workflowService = workflowService;
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


    @POST
    @Path("/workflow/{name}")
    @Produces({MediaType.TEXT_PLAIN})
    @ApiOperation("Start a new workflow. Returns the ID of the workflow instance that can be later used for tracking")
    public String startWorkflow(@PathParam("name") String name,
                                @QueryParam("version") Integer version,
                                @QueryParam("correlationId") String correlationId,@HeaderParam("x-TracingId") String tracingId,
                                Map<String, Object> input) {

        if(StringUtils.isBlank(tracingId)) {
            tracingId = IDGenerator.generate();
        }
        input.put(TRACING_ID, tracingId);
        LOGGER.info("Received request with input {} tracingId {} correlationId {}, name {}, version {}", input, tracingId, correlationId, name, version);
        return workflowService.startWorkflow(name, version, correlationId, input);
    }
}

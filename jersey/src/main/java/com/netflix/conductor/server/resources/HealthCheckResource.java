package com.netflix.conductor.server.resources;

import com.netflix.runtime.health.api.HealthCheckAggregator;
import com.netflix.runtime.health.api.HealthCheckStatus;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.swagger.annotations.Api;

@Api(value = "/health", produces = MediaType.APPLICATION_JSON, tags = "Health Check")
@Path("/health")
@Produces({MediaType.APPLICATION_JSON})
@Singleton
public class HealthCheckResource {
    private final HealthCheckAggregator healthCheck;

    @Inject
    public HealthCheckResource(HealthCheckAggregator healthCheck) {
        this.healthCheck = healthCheck;
    }

    @GET
    public HealthCheckStatus doCheck() throws Exception {
        return healthCheck.check().get();
    }

    @GET
    @Path("liveness")
    public Response doLiveness() throws Exception {
        HealthCheckStatus status = healthCheck.check().get();
        if (status.isHealthy()) {
            return Response.ok(status).build();
        }

        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(status).build();
    }

    @GET
    @Path("readiness")
    public Response doReadiness() throws Exception {
        return doLiveness();
    }

}

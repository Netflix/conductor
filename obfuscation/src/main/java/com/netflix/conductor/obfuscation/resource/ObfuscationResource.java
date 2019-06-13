package com.netflix.conductor.obfuscation.resource;

import com.netflix.conductor.obfuscation.service.ObfuscationResourceService;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

public class ObfuscationResource {

    private ObfuscationResourceService obfuscationResourceService;

    @Inject
    public ObfuscationResource(ObfuscationResourceService obfuscationResourceService) {
        this.obfuscationResourceService = obfuscationResourceService;
    }

    @POST
    @ApiOperation("obfuscate the defined fields on workflowDefinition to all workflows with given workflowName and version")
    @Consumes(MediaType.WILDCARD)
    @Path("/obfuscation")
    public void obfuscateWorkflows(@QueryParam("workflowName") String name, @QueryParam("version") Integer version) {
        obfuscationResourceService.obfuscateWorkflows(name, version);
    }

}

package com.netflix.conductor.server.resources;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.ErrorLookup;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.ErrorLookupDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Api(value = "/errors", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON, tags = "Error Lookup")
@Path("/errors")
@Produces({ MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_JSON })
@Singleton

public class ErrorLookupResource {
    private static Logger logger = LoggerFactory.getLogger(ErrorLookupResource.class);
    private ExecutionService service;
    private ErrorLookupDAO errorLookupDAO;
    private Configuration config;
    private String version;
    private String buildDate;

    @Inject
    public ErrorLookupResource(Configuration config, ErrorLookupDAO errorLookupDAO) {
        this.config = config;
        this.errorLookupDAO = errorLookupDAO;
        this.version = "UNKNOWN";
        this.buildDate = "UNKNOWN";

        try {
            InputStream propertiesIs = this.getClass().getClassLoader().getResourceAsStream("META-INF/conductor-core.properties");
            Properties prop = new Properties();
            prop.load(propertiesIs);
            this.version = prop.getProperty("Implementation-Version");
            this.buildDate = prop.getProperty("Build-Date");
        }catch(Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @GET
    @Path("/list")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get all the error lookups")
    public List<ErrorLookup> getErrors() {
        logger.debug("Called getErrors");
        return errorLookupDAO.getErrors();
    }

    @POST
    @Path("/lookup")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Lookup a particular error")
    public List<ErrorLookup> getErrors(@QueryParam("error") String error) {
        return errorLookupDAO.getErrorMatching(error);
    }

    @POST
    @Path("/lookup/{workflowname}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Lookup a particular error")
    public List<ErrorLookup> getErrors(@PathParam("workflowname") String workflowname, @QueryParam("error") String error ) {
        return errorLookupDAO.getErrorMatching(workflowname, error);
    }

    @GET
    @Path("/lookup/code/{error_code}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Lookup a particular error")
    public List<ErrorLookup> getErrorByCode(@PathParam("error_code") String errorCode) {
        return errorLookupDAO.getErrorByCode(errorCode);
    }

    @POST
    @Path("/error")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new error")
    public void addError(ErrorLookup errorLookup) {
        logger.debug("Called getErrors");
        errorLookupDAO.addError(errorLookup);
    }

    @PUT
    @Path("/error")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new error")
    public void updateError(ErrorLookup errorLookup) {
        logger.debug("Called getErrors");
        errorLookupDAO.addError(errorLookup);
    }

}

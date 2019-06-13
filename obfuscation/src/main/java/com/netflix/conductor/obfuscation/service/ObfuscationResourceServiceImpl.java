package com.netflix.conductor.obfuscation.service;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.WorkflowObfuscationQueuePublisher;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.service.WorkflowBulkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ObfuscationResourceServiceImpl implements ObfuscationResourceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObfuscationResourceServiceImpl.class);

    private WorkflowObfuscationQueuePublisher publisher;
    private MetadataDAO metadataDAO;
    private ExecutionDAOFacade executionDAOFacade;

    @Inject
    public ObfuscationResourceServiceImpl(WorkflowObfuscationQueuePublisher publisher, MetadataDAO metadataDAO, ExecutionDAOFacade executionDAOFacade) {
        this.publisher = publisher;
        this.metadataDAO = metadataDAO;
        this.executionDAOFacade = executionDAOFacade;
    }

    /**
     * @param name Name of the Workflow
     * @param version Version of the Workflow
     * @throws ApplicationException If there was an error - caller should retry in this case.
     */
    public void obfuscateWorkflows(String name, Integer version) {
        validateInput(name, version);
        WorkflowDef workflowDef = getWorkflowDef(name, version);
        long totalHits = searchWorkflowsToPublish(name, version, 0).getTotalHits();

        if(totalHits > 0) {
            int processedWorkflows = 0;
            while(processedWorkflows < totalHits) {
                SearchResult<String> results = searchWorkflowsToPublish(name, version, processedWorkflows);

                List<String> workflowIds = results.getResults();

                publisher.publishAll(workflowIds, workflowDef);
                processedWorkflows = processedWorkflows + workflowIds.size();
            }
        } else {
            LOGGER.info("no workflows were found to be obfuscated");
        }
    }

    private void validateInput(String name, Integer version) {
        if(version == null || version <= 0) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT, "workflow version must be present and cannot have 0 or below value");
        }
        if(isEmpty(name)) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT, "workflow name must be present");
        }
    }

    private WorkflowDef getWorkflowDef(String name, Integer version) {
        Optional<WorkflowDef> workflowDef = metadataDAO.get(name, version);
        if(workflowDef.isPresent()) {
            return workflowDef.get();
        } else {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT, "workflowDefinition does not exists");
        }
    }

    private SearchResult<String> searchWorkflowsToPublish(String name, Integer version, int start) {
        return executionDAOFacade.searchWorkflows(null, String.format("workflowType:%s AND version:%s", name, version), start, 100, null);
    }

}

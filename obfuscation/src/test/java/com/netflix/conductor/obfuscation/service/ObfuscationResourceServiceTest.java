package com.netflix.conductor.obfuscation.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.WorkflowObfuscationQueuePublisher;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.dao.MetadataDAO;
import org.junit.Test;

import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class ObfuscationResourceServiceTest {

    private WorkflowObfuscationQueuePublisher publisher = mock(WorkflowObfuscationQueuePublisher.class);
    private MetadataDAO metadataDAO = mock(MetadataDAO.class);
    private ExecutionDAOFacade executionDAOFacade = mock(ExecutionDAOFacade.class);
    private ObfuscationResourceService obfuscationResourceService = new ObfuscationResourceServiceImpl(publisher, metadataDAO, executionDAOFacade);



    @Test
    public void testObfuscateWorkflows() throws JsonProcessingException {
        String workflowName = "test_workflow";
        int workflowVersion = 1;
        WorkflowDef workflowDefinition = new WorkflowDef();
        String query = String.format("workflowType:%s AND version:%s", workflowName, workflowVersion);
        String workflowId1 = "workflowId1";
        String workflowId2 = "workflowId2";


        when(metadataDAO.get(workflowName, workflowVersion)).thenReturn(Optional.of(workflowDefinition));
        when(executionDAOFacade.searchWorkflows(null, query, 0, 100, null)).thenReturn(
                new SearchResult<>(2, singletonList(workflowId1)));
        when(executionDAOFacade.searchWorkflows(null, query, 1, 100, null)).thenReturn(
                new SearchResult<>(2, singletonList(workflowId2)));

        obfuscationResourceService.obfuscateWorkflows(workflowName, workflowVersion);

        verify(publisher, times(1)).publishAll(singletonList(workflowId1), workflowDefinition);
        verify(publisher, times(1)).publishAll(singletonList(workflowId2), workflowDefinition);
    }

    @Test
    public void testObfuscateWorkflowsWithNoWorkflowsFound() {
        String workflowName = "test_workflow";
        int workflowVersion = 1;
        WorkflowDef workflowDefinition = new WorkflowDef();
        String query = String.format("workflowType:%s AND version:%s", workflowName, workflowVersion);


        when(metadataDAO.get(workflowName, workflowVersion)).thenReturn(Optional.of(workflowDefinition));
        when(executionDAOFacade.searchWorkflows(null, query, 0, 100, null)).thenReturn(
                new SearchResult<>(0, emptyList()));

        obfuscationResourceService.obfuscateWorkflows(workflowName, workflowVersion);

        verify(publisher, times(0)).publishAll(anyListOf(String.class), anyObject());
    }

    @Test(expected = ApplicationException.class)
    public void testObfuscateWorkflowsFailingWithExceptionForNotFoundWorkflowDef() {
        String workflowName = "test_workflow";
        int workflowVersion = 1;

        when(metadataDAO.get(workflowName, workflowVersion)).thenReturn(Optional.empty());

        obfuscationResourceService.obfuscateWorkflows(workflowName, workflowVersion);
    }

    @Test(expected = ApplicationException.class)
    public void testObfuscateWorkflowsFailingWithExceptionForEmptyWorkflowName() {
        obfuscationResourceService.obfuscateWorkflows("", 1);
    }

    @Test(expected = ApplicationException.class)
    public void testObfuscateWorkflowsFailingWithExceptionForNullWorkflowName() {
        obfuscationResourceService.obfuscateWorkflows(null, 1);
    }

    @Test(expected = ApplicationException.class)
    public void testObfuscateWorkflowsFailingWithExceptionForNullWorkflowVersion() {
        obfuscationResourceService.obfuscateWorkflows("workflowName", null);
    }

    @Test(expected = ApplicationException.class)
    public void testObfuscateWorkflowsFailingWithExceptionForWorkflowVersionEqualTo0() {
        obfuscationResourceService.obfuscateWorkflows("workflowName", 0);
    }

    @Test(expected = ApplicationException.class)
    public void testObfuscateWorkflowsFailingWithExceptionForInvalidWorkflowVersion() {
        obfuscationResourceService.obfuscateWorkflows("workflowName", -1);
    }


}

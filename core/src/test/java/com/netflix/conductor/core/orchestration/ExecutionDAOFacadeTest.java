/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.orchestration;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.execution.TestDeciderService;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutionDAOFacadeTest {

    private ExecutionDAO executionDAO;
    private IndexDAO indexDAO;
    private ObjectMapper objectMapper;
    private ExecutionDAOFacade executionDAOFacade;

    @Before
    public void setUp() {
        executionDAO = mock(ExecutionDAO.class);
        indexDAO = mock(IndexDAO.class);
        objectMapper = new JsonMapperProvider().get();
        executionDAOFacade = new ExecutionDAOFacade(executionDAO, indexDAO, objectMapper);
    }

    @Test
    @Ignore
    public void tesGetWorkflowById() throws Exception {
        when(executionDAO.getWorkflow(any(), anyBoolean())).thenReturn(new Workflow());
        Workflow workflow = executionDAOFacade.getWorkflowById("workflowId", true);
        assertNotNull(workflow);
        verify(indexDAO, never()).get(any(), any());

        when(executionDAO.getWorkflow(any(), anyBoolean())).thenReturn(null);
        InputStream stream = ExecutionDAOFacadeTest.class.getResourceAsStream("/test.json");
        byte[] bytes = IOUtils.toByteArray(stream);
        String jsonString = new String(bytes);
        when(indexDAO.get(any(), any())).thenReturn(jsonString);
        workflow = executionDAOFacade.getWorkflowById("workflowId", true);
        assertNotNull(workflow);
        verify(indexDAO, times(1)).get(any(), any());
    }

    @Test
    public void testGetWorkflowsByCorrelationId() {
        when(executionDAO.canSearchAcrossWorkflows()).thenReturn(true);
        when(executionDAO.getWorkflowsByCorrelationId(any(), anyBoolean())).thenReturn(Collections.singletonList(new Workflow()));
        List<Workflow> workflows = executionDAOFacade.getWorkflowsByCorrelationId("correlationId", true);
        assertNotNull(workflows);
        assertEquals(1, workflows.size());
        verify(indexDAO, never()).searchWorkflows(anyString(), anyString(), anyInt(), anyInt(), any());

        when(executionDAO.canSearchAcrossWorkflows()).thenReturn(false);
        List<String> workflowIds = new ArrayList<>();
        workflowIds.add("workflowId");
        SearchResult<String> searchResult = new SearchResult<>();
        searchResult.setResults(workflowIds);
        when(indexDAO.searchWorkflows(anyString(), anyString(), anyInt(), anyInt(), any())).thenReturn(searchResult);
        when(executionDAO.getWorkflow("workflowId", true)).thenReturn(new Workflow());
        workflows = executionDAOFacade.getWorkflowsByCorrelationId("correlationId", true);
        assertNotNull(workflows);
        assertEquals(1, workflows.size());
    }

    @Test
    @Ignore
    public void testRemoveWorkflow() {
        when(executionDAO.getWorkflow(anyString(), anyBoolean())).thenReturn(new Workflow());
        executionDAOFacade.removeWorkflow("workflowId", false);
        verify(indexDAO, never()).updateWorkflow(any(), any(), any());
        verify(indexDAO, times(1)).removeWorkflow(anyString());
    }

    @Test
    @Ignore
    public void testArchiveWorkflow() throws Exception {
        InputStream stream = TestDeciderService.class.getResourceAsStream("/test.json");
        Workflow workflow = objectMapper.readValue(stream, Workflow.class);

        when(executionDAO.getWorkflow(anyString(), anyBoolean())).thenReturn(workflow);
        executionDAOFacade.removeWorkflow("workflowId", true);
        verify(indexDAO, times(1)).updateWorkflow(any(), any(), any());
        verify(indexDAO, never()).removeWorkflow(any());
    }

    @Test
    @Ignore
    public void testAddEventExecution() {
        when(executionDAO.addEventExecution(any())).thenReturn(false);
        boolean added = executionDAOFacade.addEventExecution(new EventExecution());
        assertFalse(added);
        verify(indexDAO, never()).addEventExecution(any());

        when(executionDAO.addEventExecution(any())).thenReturn(true);
        added = executionDAOFacade.addEventExecution(new EventExecution());
        assertTrue(added);
        verify(indexDAO, times(1)).addEventExecution(any());
    }
}
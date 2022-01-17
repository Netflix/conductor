/*
 * Copyright 2021 Netflix, Inc.
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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.execution.TestDeciderService;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ContextConfiguration(classes = {TestObjectMapperConfiguration.class})
@RunWith(SpringRunner.class)
public class ExecutionDAOFacadeTest {

    private ExecutionDAO executionDAO;
    private IndexDAO indexDAO;
    private ExecutionDAOFacade executionDAOFacade;

    @Autowired private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        executionDAO = mock(ExecutionDAO.class);
        QueueDAO queueDAO = mock(QueueDAO.class);
        indexDAO = mock(IndexDAO.class);
        RateLimitingDAO rateLimitingDao = mock(RateLimitingDAO.class);
        ConcurrentExecutionLimitDAO concurrentExecutionLimitDAO =
                mock(ConcurrentExecutionLimitDAO.class);
        PollDataDAO pollDataDAO = mock(PollDataDAO.class);
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.isEventExecutionIndexingEnabled()).thenReturn(true);
        when(properties.isAsyncIndexingEnabled()).thenReturn(true);
        executionDAOFacade =
                new ExecutionDAOFacade(
                        executionDAO,
                        queueDAO,
                        indexDAO,
                        rateLimitingDao,
                        concurrentExecutionLimitDAO,
                        pollDataDAO,
                        objectMapper,
                        properties);
    }

    @Test
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
        when(executionDAO.getWorkflowsByCorrelationId(any(), any(), anyBoolean()))
                .thenReturn(Collections.singletonList(new Workflow()));
        List<Workflow> workflows =
                executionDAOFacade.getWorkflowsByCorrelationId(
                        "workflowName", "correlationId", true);
        assertNotNull(workflows);
        assertEquals(1, workflows.size());
        verify(indexDAO, never())
                .searchWorkflows(anyString(), anyString(), anyInt(), anyInt(), any());

        when(executionDAO.canSearchAcrossWorkflows()).thenReturn(false);
        List<String> workflowIds = new ArrayList<>();
        workflowIds.add("workflowId");
        SearchResult<String> searchResult = new SearchResult<>();
        searchResult.setResults(workflowIds);
        when(indexDAO.searchWorkflows(anyString(), anyString(), anyInt(), anyInt(), any()))
                .thenReturn(searchResult);
        when(executionDAO.getWorkflow("workflowId", true)).thenReturn(new Workflow());
        workflows =
                executionDAOFacade.getWorkflowsByCorrelationId(
                        "workflowName", "correlationId", true);
        assertNotNull(workflows);
        assertEquals(1, workflows.size());
    }

    @Test
    public void testRemoveWorkflow() {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.COMPLETED);
        when(executionDAO.getWorkflow(anyString(), anyBoolean())).thenReturn(workflow);
        executionDAOFacade.removeWorkflow("workflowId", false);
        verify(indexDAO, never()).updateWorkflow(any(), any(), any());
        verify(indexDAO, times(1)).asyncRemoveWorkflow(workflow.getWorkflowId());
    }

    @Test
    public void testArchiveWorkflow() throws Exception {
        InputStream stream = TestDeciderService.class.getResourceAsStream("/completed.json");
        Workflow workflow = objectMapper.readValue(stream, Workflow.class);

        when(executionDAO.getWorkflow(anyString(), anyBoolean())).thenReturn(workflow);
        executionDAOFacade.removeWorkflow("workflowId", true);
        verify(indexDAO, times(1)).updateWorkflow(any(), any(), any());
        verify(indexDAO, never()).removeWorkflow(any());
    }

    @Test
    public void testAddEventExecution() {
        when(executionDAO.addEventExecution(any())).thenReturn(false);
        boolean added = executionDAOFacade.addEventExecution(new EventExecution());
        assertFalse(added);
        verify(indexDAO, never()).addEventExecution(any());

        when(executionDAO.addEventExecution(any())).thenReturn(true);
        added = executionDAOFacade.addEventExecution(new EventExecution());
        assertTrue(added);
        verify(indexDAO, times(1)).asyncAddEventExecution(any());
    }
}

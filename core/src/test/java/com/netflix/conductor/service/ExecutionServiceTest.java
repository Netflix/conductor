package com.netflix.conductor.service;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.dao.QueueDAO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class ExecutionServiceTest {

    @Mock
    private WorkflowExecutor workflowExecutor;
    @Mock
    private ExecutionDAOFacade executionDAOFacade;
    @Mock
    private QueueDAO queueDAO;
    @Mock
    private ConductorProperties conductorProperties;
    @Mock
    private ExternalPayloadStorage externalPayloadStorage;
    @Mock
    private SystemTaskRegistry systemTaskRegistry;

    private ExecutionService executionService;

    private Workflow workflow1;
    private Workflow workflow2;
    private Task taskWorkflow1;
    private Task taskWorkflow2;
    private final List<String> sort = Collections.singletonList("Sort");

    @Before
    public void setup() {
        when(conductorProperties.getTaskExecutionPostponeDuration()).thenReturn(Duration.ofSeconds(60));
        executionService = new ExecutionService(workflowExecutor, executionDAOFacade, queueDAO,
                conductorProperties, externalPayloadStorage, systemTaskRegistry);
        WorkflowDef workflowDef = new WorkflowDef();
        workflow1 = new Workflow();
        workflow1.setWorkflowId("wf1");
        workflow1.setWorkflowDefinition(workflowDef);
        workflow2 = new Workflow();
        workflow2.setWorkflowId("wf2");
        workflow2.setWorkflowDefinition(workflowDef);
        taskWorkflow1 = new Task();
        taskWorkflow1.setTaskId("task1");
        taskWorkflow1.setWorkflowInstanceId("wf1");
        taskWorkflow2 = new Task();
        taskWorkflow2.setTaskId("task2");
        taskWorkflow2.setWorkflowInstanceId("wf2");
    }


    @Test
    public void searchTest() {
        when(executionDAOFacade.searchWorkflows("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(workflow1.getWorkflowId(), workflow2.getWorkflowId())));
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        when(executionDAOFacade.getWorkflowById(workflow2.getWorkflowId(), false)).thenReturn(workflow2);
        SearchResult<WorkflowSummary> searchResult = executionService.search("query", "*", 0, 2, sort);
        assertEquals(2, searchResult.getTotalHits());
        assertEquals(2, searchResult.getResults().size());
        assertEquals(workflow1.getWorkflowId(), searchResult.getResults().get(0).getWorkflowId());
        assertEquals(workflow2.getWorkflowId(), searchResult.getResults().get(1).getWorkflowId());
    }

    @Test
    public void searchExceptionTest() {
        when(executionDAOFacade.searchWorkflows("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(workflow1.getWorkflowId(), workflow2.getWorkflowId())));
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        when(executionDAOFacade.getWorkflowById(workflow2.getWorkflowId(), false)).thenThrow(new RuntimeException());
        SearchResult<WorkflowSummary> searchResult = executionService.search("query", "*", 0, 2, sort);
        assertEquals(1, searchResult.getTotalHits());
        assertEquals(1, searchResult.getResults().size());
        assertEquals(workflow1.getWorkflowId(), searchResult.getResults().get(0).getWorkflowId());
    }

    @Test
    public void searchV2Test() {
        when(executionDAOFacade.searchWorkflows("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(workflow1.getWorkflowId(), workflow2.getWorkflowId())));
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        when(executionDAOFacade.getWorkflowById(workflow2.getWorkflowId(), false)).thenReturn(workflow2);
        SearchResult<Workflow> searchResult = executionService.searchV2("query", "*", 0, 2, sort);
        assertEquals(2, searchResult.getTotalHits());
        assertEquals(Arrays.asList(workflow1, workflow2), searchResult.getResults());
    }

    @Test
    public void searchV2ExceptionTest() {
        when(executionDAOFacade.searchWorkflows("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(workflow1.getWorkflowId(), workflow2.getWorkflowId())));
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        when(executionDAOFacade.getWorkflowById(workflow2.getWorkflowId(), false)).thenThrow(new RuntimeException());
        SearchResult<Workflow> searchResult = executionService.searchV2("query", "*", 0, 2, sort);
        assertEquals(1, searchResult.getTotalHits());
        assertEquals(Collections.singletonList(workflow1), searchResult.getResults());
    }

    @Test
    public void searchByTasksTest() {
        when(executionDAOFacade.searchTasks("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(taskWorkflow1.getTaskId(), taskWorkflow2.getTaskId())));
        when(executionDAOFacade.getTaskById(taskWorkflow1.getTaskId())).thenReturn(taskWorkflow1);
        when(executionDAOFacade.getTaskById(taskWorkflow2.getTaskId())).thenReturn(taskWorkflow2);
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        when(executionDAOFacade.getWorkflowById(workflow2.getWorkflowId(), false)).thenReturn(workflow2);
        SearchResult<WorkflowSummary> searchResult = executionService.searchWorkflowByTasks("query", "*", 0, 2, sort);
        assertEquals(2, searchResult.getTotalHits());
        assertEquals(2, searchResult.getResults().size());
        assertEquals(workflow1.getWorkflowId(), searchResult.getResults().get(0).getWorkflowId());
        assertEquals(workflow2.getWorkflowId(), searchResult.getResults().get(1).getWorkflowId());
    }

    @Test
    public void searchByTasksExceptionTest() {
        when(executionDAOFacade.searchTasks("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(taskWorkflow1.getTaskId(), taskWorkflow2.getTaskId())));
        when(executionDAOFacade.getTaskById(taskWorkflow1.getTaskId())).thenReturn(taskWorkflow1);
        when(executionDAOFacade.getTaskById(taskWorkflow2.getTaskId())).thenThrow(new RuntimeException());
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        SearchResult<WorkflowSummary> searchResult = executionService.searchWorkflowByTasks("query", "*", 0, 2, sort);
        assertEquals(1, searchResult.getTotalHits());
        assertEquals(1, searchResult.getResults().size());
        assertEquals(workflow1.getWorkflowId(), searchResult.getResults().get(0).getWorkflowId());
    }

    @Test
    public void searchByTasksV2Test() {
        when(executionDAOFacade.searchTasks("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(taskWorkflow1.getTaskId(), taskWorkflow2.getTaskId())));
        when(executionDAOFacade.getTaskById(taskWorkflow1.getTaskId())).thenReturn(taskWorkflow1);
        when(executionDAOFacade.getTaskById(taskWorkflow2.getTaskId())).thenReturn(taskWorkflow2);
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        when(executionDAOFacade.getWorkflowById(workflow2.getWorkflowId(), false)).thenReturn(workflow2);
        SearchResult<Workflow> searchResult = executionService.searchWorkflowByTasksV2("query", "*", 0, 2, sort);
        assertEquals(2, searchResult.getTotalHits());
        assertEquals(Arrays.asList(workflow1, workflow2), searchResult.getResults());
    }

    @Test
    public void searchByTasksV2ExceptionTest() {
        when(executionDAOFacade.searchTasks("query", "*", 0, 2, sort))
                .thenReturn(new SearchResult<>(2, Arrays.asList(taskWorkflow1.getTaskId(), taskWorkflow2.getTaskId())));
        when(executionDAOFacade.getTaskById(taskWorkflow1.getTaskId())).thenReturn(taskWorkflow1);
        when(executionDAOFacade.getTaskById(taskWorkflow2.getTaskId())).thenThrow(new RuntimeException());
        when(executionDAOFacade.getWorkflowById(workflow1.getWorkflowId(), false)).thenReturn(workflow1);
        SearchResult<Workflow> searchResult = executionService.searchWorkflowByTasksV2("query", "*", 0, 2, sort);
        assertEquals(1, searchResult.getTotalHits());
        assertEquals(Collections.singletonList(workflow1), searchResult.getResults());
    }
}

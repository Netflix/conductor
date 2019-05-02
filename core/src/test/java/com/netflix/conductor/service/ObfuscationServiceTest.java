package com.netflix.conductor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.exception.ObfuscationServiceException;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class ObfuscationServiceTest {

    private ObfuscationServiceImpl obfuscationService;
    private ObjectMapper mapper = new JsonMapperProvider().get();
    private ExecutionDAO executionDAO = mock(ExecutionDAO.class);
    private IndexDAO indexDAO = mock(IndexDAO.class);
    private MetadataService metadataService = mock(MetadataService.class);
    private Configuration config = mock(Configuration.class);
    private Workflow workflow;
    private InputStream workflowJsonFile;
    private static String workflowId = "f580ba3a-7c03-4db3-aab0-7d35f28aeb14";

    @Before
    public void setup() throws Exception {
        when(config.getProperty("workflow.obfuscation.field.replace.value", "***")).thenReturn("***");
        obfuscationService = new ObfuscationServiceImpl(mapper, executionDAO, indexDAO, metadataService, config);
        workflowJsonFile = ObfuscationServiceTest.class.getResourceAsStream("/test_workflow.json");
        workflow = mapper.readValue(workflowJsonFile, Workflow.class);
    }

    @Test
    public void should_obfuscate_selected_fields() throws Exception {
        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);
        when(metadataService.getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion()))
                .thenReturn(workflow.getWorkflowDefinition());

        obfuscationService.obfuscateFields(workflowId);

        DocumentContext workflowJson = JsonPath.parse(mapper.writeValueAsString(workflow));
        workflowJson.set("output.response.body", "***");

        DocumentContext tasksJson = JsonPath.parse(mapper.writeValueAsString(workflow.getTasks()));
        tasksJson.set("[1].inputData.http_request.headers.x-workflow-id", "***");


        Workflow expectedWorkflow = mapper.convertValue(workflowJson.json(), Workflow.class);
        List<Task> expectedTasks = Arrays.asList(mapper.readValue(tasksJson.jsonString(), Task[].class));


        verify(executionDAO, times(1)).getWorkflow(workflowId, true);
        verify(metadataService, times(1)).getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion());
        verify(executionDAO, times(1)).updateTasks(expectedTasks);
        verify(executionDAO, times(1)).updateWorkflow(expectedWorkflow);
        verify(indexDAO, times(1)).indexWorkflow(expectedWorkflow);
    }

    @Test
    public void should_not_obfuscate_if_there_are_no_given_fields() {
        workflow.getWorkflowDefinition().setObfuscationFields(null);

        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);
        when(metadataService.getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion()))
                .thenReturn(workflow.getWorkflowDefinition());

        obfuscationService.obfuscateFields(workflowId);

        verify(executionDAO, times(1)).getWorkflow(anyString(), anyBoolean());
        verify(metadataService, times(1)).getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion());
        verify(executionDAO, times(0)).updateWorkflow(anyObject());
        verify(executionDAO, times(0)).updateTasks(anyObject());
        verify(indexDAO, times(0)).indexWorkflow(anyObject());
    }

    @Test
    public void should_not_obfuscate_if_fields_are_empty() {
        workflow.getWorkflowDefinition().setObfuscationFields(new HashMap<>());

        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);
        when(metadataService.getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion()))
                .thenReturn(workflow.getWorkflowDefinition());

        obfuscationService.obfuscateFields(workflowId);

        verify(executionDAO, times(1)).getWorkflow(anyString(), anyBoolean());
        verify(metadataService, times(1)).getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion());
        verify(executionDAO, times(0)).updateWorkflow(anyObject());
        verify(executionDAO, times(0)).updateTasks(anyObject());
        verify(indexDAO, times(0)).indexWorkflow(anyObject());
    }

    @Test
    public void should_not_update_workflow_if_workflow_field_is_incorrect() {
        workflow.getWorkflowDefinition().setObfuscationFields(ImmutableMap.of("workflowFields", "incorrectField"));

        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);
        when(metadataService.getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion()))
                .thenReturn(workflow.getWorkflowDefinition());

        obfuscationService.obfuscateFields(workflowId);

        verify(executionDAO, times(1)).getWorkflow(workflowId, true);
        verify(metadataService, times(1)).getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion());
        verify(executionDAO, times(0)).updateWorkflow(anyObject());
        verify(executionDAO, times(0)).updateTasks(anyObject());
        verify(indexDAO, times(0)).indexWorkflow(anyObject());
    }

    @Test
    public void should_not_update_workflow_if_task_field_is_incorrect() {
        workflow.getWorkflowDefinition().setObfuscationFields(ImmutableMap.of("taskFields", "Request02.inputData.incorrectField"));

        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);
        when(metadataService.getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion()))
                .thenReturn(workflow.getWorkflowDefinition());

        obfuscationService.obfuscateFields(workflowId);

        verify(executionDAO, times(1)).getWorkflow(workflowId, true);
        verify(metadataService, times(1)).getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion());
        verify(executionDAO, times(0)).updateWorkflow(anyObject());
        verify(executionDAO, times(0)).updateTasks(anyObject());
        verify(indexDAO, times(0)).indexWorkflow(anyObject());
    }

    @Test(expected = ObfuscationServiceException.class)
    public void should_throw_service_exception_if_workflow_is_not_found() {
        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(null);

        obfuscationService.obfuscateFields(workflowId);
    }

    @Test(expected = ObfuscationServiceException.class)
    public void should_throw_service_exception_if_workflowDef_is_not_found() {
        when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);
        when(metadataService.getWorkflowDef(workflow.getWorkflowName(), workflow.getVersion()))
                .thenReturn(null);

        obfuscationService.obfuscateFields(workflowId);
    }
}



package com.netflix.conductor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.exception.ObfuscationServiceException;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;

public class ObfuscationServiceTest {

    private ObfuscationServiceImpl obfuscationService;
    private ObjectMapper mapper = new JsonMapperProvider().get();
    private ExecutionDAO executionDAO = Mockito.mock(ExecutionDAO.class);
    private IndexDAO indexDAO = Mockito.mock(IndexDAO.class);
    private Workflow workflow;
    private InputStream workflowJsonFile;
    private static String workflowId = "f580ba3a-7c03-4db3-aab0-7d35f28aeb14";

    @Before
    public void setup() throws Exception {
        obfuscationService = new ObfuscationServiceImpl(mapper, executionDAO, indexDAO);
        workflowJsonFile = ObfuscationServiceTest.class.getResourceAsStream("/test_workflow.json");
        workflow = mapper.readValue(workflowJsonFile, Workflow.class);
    }

    @Test
    public void should_obfuscate_selected_fields() throws JsonProcessingException {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(ImmutableMap.of("WorkflowFields", singletonList("output.response.body"),
                "TasksFields", singletonList("Request02.inputData.http_request.headers.x-workflow-id")));

        Mockito.when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);

        obfuscationService.obfuscateFields(workflowId, workflowDef);


        DocumentContext workflowJson = JsonPath.parse(mapper.writeValueAsString(workflow));
        workflowJson
                .set("output.response.body", "***")
                .set("tasks[1].inputData.http_request.headers.x-workflow-id", "***");

        Workflow expectedWorkflow = mapper.convertValue(workflowJson.json(), Workflow.class);

        Mockito.verify(executionDAO, times(1)).getWorkflow(workflowId, true);
        Mockito.verify(executionDAO, times(1)).updateWorkflow(expectedWorkflow);
        Mockito.verify(indexDAO, times(1)).asyncIndexWorkflow(expectedWorkflow);
    }

    @Test
    public void should_not_obfuscate_if_there_are_no_given_fields() {
        WorkflowDef workflowDef = new WorkflowDef();

        Mockito.when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);

        obfuscationService.obfuscateFields(workflowId, workflowDef);

        Mockito.verify(executionDAO, times(0)).getWorkflow(anyString(), anyBoolean());
        Mockito.verify(executionDAO, times(0)).updateWorkflow(anyObject());
        Mockito.verify(indexDAO, times(0)).asyncIndexWorkflow(anyObject());
    }

    @Test
    public void should_not_obfuscate_if_fields_are_empty() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(new HashMap<>());

        Mockito.when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);

        obfuscationService.obfuscateFields(workflowId, workflowDef);

        Mockito.verify(executionDAO, times(0)).getWorkflow(anyString(), anyBoolean());
        Mockito.verify(executionDAO, times(0)).updateWorkflow(anyObject());
        Mockito.verify(indexDAO, times(0)).asyncIndexWorkflow(anyObject());
    }

    @Test(expected = ObfuscationServiceException.class)
    public void should_throw_service_exception_if_workflow_field_is_incorrect() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(ImmutableMap.of("WorkflowFields", singletonList("incorrectField")));

        Mockito.when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);

        obfuscationService.obfuscateFields(workflowId, workflowDef);
    }

    @Test(expected = ObfuscationServiceException.class)
    public void should_throw_service_exception_if_workflow_task_field_is_incorrect() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(ImmutableMap.of("WorkflowFields", singletonList("output.response.body"),
                "TasksFields", singletonList("Request02.inputData.incorrectField")));

        Mockito.when(executionDAO.getWorkflow(workflowId, true)).thenReturn(workflow);

        obfuscationService.obfuscateFields(workflowId, workflowDef);
    }

    @Test(expected = ObfuscationServiceException.class)
    public void should_throw_service_exception_if_workflow_is_not_found() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(ImmutableMap.of("WorkflowFields", singletonList("output.response.body"),
                "TasksFields", singletonList("Request02.inputData.http_request.headers.x-workflow-id")));

        Mockito.when(executionDAO.getWorkflow(workflowId, true)).thenReturn(null);

        obfuscationService.obfuscateFields(workflowId, workflowDef);
    }
}



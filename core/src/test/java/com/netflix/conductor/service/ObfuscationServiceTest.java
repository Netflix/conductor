package com.netflix.conductor.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static java.util.Collections.singletonList;

public class ObfuscationServiceTest {

    private ObfuscationServiceImpl obfuscationService;
    private ObjectMapper mapper = new JsonMapperProvider().get();
    private ExecutionDAO executionDAO = Mockito.mock(ExecutionDAO.class);
    private Workflow workflow;

    @Before
    public void setup() throws Exception {
        obfuscationService = new ObfuscationServiceImpl(mapper, executionDAO);
        InputStream workflowJson = ObfuscationServiceTest.class.getResourceAsStream("/test_workflow.json");
        workflow = mapper.readValue(workflowJson, Workflow.class);
    }

    @Test
    public void should_obfuscate_selected_fields() {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(ImmutableMap.of("WorkflowFields", singletonList("output.Request01.response"),
                "TasksFields", singletonList("Request01.inputData.http_request.headers.x-workflow-id")));

        Mockito.when(executionDAO.getWorkflow("8684d51a-73b5-4b5f-bdc7-b9eba85441d3", true)).thenReturn(workflow);

        obfuscationService.obfuscateFields("8684d51a-73b5-4b5f-bdc7-b9eba85441d3", workflowDef);
    }


}

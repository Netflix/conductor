package com.netflix.conductor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.LinkedHashMap;

public class ObfuscationServiceImpl implements ObfuscationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObfuscationServiceImpl.class);
    private static final String OBFUSCATION_VALUE = "***";
    private static final String REFERENCE_TASK_NAME_QUERY = "$.[?(@.referenceTaskName == '%s')]";
    private static final String JSON_TASK_INDEX = "[0].";
    private static final String WORKFLOW_OBFUSCATION_FIELDS = "WorkflowFields";
    private static final String TASKS_OBFUSCATION_FIELDS = "TasksFields";
    private final ObjectMapper objectMapper;
    private final ExecutionDAO executionDAO;

    @Inject
    public ObfuscationServiceImpl(ObjectMapper objectMapper, ExecutionDAO executionDAO) {
        this.objectMapper = objectMapper;
        this.executionDAO = executionDAO;
    }


    //TODO: run this async
    @Override
    public void obfuscateFields(String workflowId, WorkflowDef workflowDef) {
        Workflow workflow = getWorkflow(workflowId);
        DocumentContext jsonWorkflow = JsonPath.parse(convertToJson(workflow));
        DocumentContext workflowTasks = JsonPath.parse(convertToJson(workflow.getTasks()));

        workflowDef.getObfuscationFields().get(WORKFLOW_OBFUSCATION_FIELDS).forEach(field -> obfuscateWorkflowField(field, jsonWorkflow));
        workflowDef.getObfuscationFields().get(TASKS_OBFUSCATION_FIELDS).forEach(field -> obfuscateTaskField(field, workflowTasks));

        jsonWorkflow.set("tasks", workflowTasks.json());

        System.out.println(jsonWorkflow.jsonString());

//        executionDAO.updateWorkflow(objectMapper.convertValue(jsonWorkflow, Workflow.class));
        //TODO: add indexDAO

    }

    @Override
    public void obfuscateFieldsByWorkflowDef(String name, Integer version) {

    }

    private void obfuscateWorkflowField(String field, DocumentContext workflow) {
        LinkedHashMap fieldToObfuscate = workflow.read(field);
        if(!fieldToObfuscate.isEmpty()) {
            workflow.set(field, OBFUSCATION_VALUE);
        }
    }

    private void obfuscateTaskField(String field, DocumentContext workflowTasks) {
        JSONArray task = findTask(buildQuery(getReferenceTaskName(field)), workflowTasks);
        if(!task.isEmpty()) {
            String fieldToObfuscate = getFieldToObfuscate(field);
            try {
                JsonPath.parse(task).set(fieldToObfuscate, OBFUSCATION_VALUE);
            } catch (Exception e) {
                LOGGER.info("obfuscation failed for field: {} with exception: {}", fieldToObfuscate, e);
            }
        }
    }

    private JSONArray findTask(String query, DocumentContext workflowTasks) {
        return workflowTasks.read(query);
    }

    private String buildQuery(String referenceTaskName) {
        return String.format(REFERENCE_TASK_NAME_QUERY, referenceTaskName);
    }

    private String getReferenceTaskName(String obfuscationField) {
        return obfuscationField.split("\\.")[0];
    }

    private String getFieldToObfuscate(String obfuscationField) {
       return JSON_TASK_INDEX + obfuscationField.replaceFirst(".*?\\.",  "");

    }


    private String convertToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Workflow getWorkflow(String workflowId) {
        return executionDAO.getWorkflow(workflowId, true);
    }
}

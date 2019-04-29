package com.netflix.conductor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.exception.ObfuscationServiceException;
import net.minidev.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;

import static java.util.Arrays.asList;

@Singleton
public class ObfuscationServiceImpl implements ObfuscationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObfuscationServiceImpl.class);
    //TODO: use system prop here
    private static final String OBFUSCATION_VALUE = "***";
    private static final String REFERENCE_TASK_NAME_QUERY = "$.[?(@.referenceTaskName == '%s')]";
    private static final String JSON_TASK_INDEX = "[0].";
    private static final String WORKFLOW_OBFUSCATION_FIELDS = "workflowFields";
    private static final String TASKS_OBFUSCATION_FIELDS = "taskFields";
    private static final String WORKFLOW_TASKS_FIELD = "tasks";
    private final ObjectMapper objectMapper;
    private final ExecutionDAO executionDAO;
    private final IndexDAO indexDAO;

    @Inject
    public ObfuscationServiceImpl(ObjectMapper objectMapper, ExecutionDAO executionDAO, IndexDAO indexDAO) {
        this.objectMapper = objectMapper;
        this.executionDAO = executionDAO;
        this.indexDAO = indexDAO;
    }

    @Override
    public void obfuscateFields(String workflowId) {
        Workflow workflow = getWorkflow(workflowId);

        if(hasFieldsToObfuscate(workflow.getWorkflowDefinition())) {
            DocumentContext jsonWorkflow = JsonPath.parse(convertToJson(workflow));
            DocumentContext jsonWorkflowTasks = JsonPath.parse(convertToJson(workflow.getTasks()));

            List<String> workflowChangedFields = new ArrayList<>();

            getObfuscationFieldsList(workflow.getWorkflowDefinition(), WORKFLOW_OBFUSCATION_FIELDS)
                    .forEach(field -> obfuscateWorkflowField(field, jsonWorkflow, workflowChangedFields));
            getObfuscationFieldsList(workflow.getWorkflowDefinition(), TASKS_OBFUSCATION_FIELDS)
                    .forEach(field -> obfuscateTaskField(field, jsonWorkflowTasks, workflowChangedFields));

            if(!workflowChangedFields.isEmpty()) {
                if(workflowChangedFields.contains(WORKFLOW_TASKS_FIELD)) {
                    List<Task> updatedTasks = convertJsonTasksToTaskList(jsonWorkflowTasks);
                    executionDAO.updateTasks(updatedTasks);
                }
                Workflow updatedWorkflow = objectMapper.convertValue(jsonWorkflow.json(), Workflow.class);
                executionDAO.updateWorkflow(updatedWorkflow);
                indexDAO.asyncIndexWorkflow(updatedWorkflow);
            }
        }
    }

    @Override
    public void obfuscateFieldsByWorkflowDef(String name, Integer version) {}

    private void obfuscateWorkflowField(String field, DocumentContext workflow, List<String> workflowChangedFields) {
        try{
            LinkedHashMap fieldToObfuscate = workflow.read(field);
            if(!fieldToObfuscate.isEmpty()) {
                workflow.set(field, OBFUSCATION_VALUE);
                workflowChangedFields.add(getRootField(field));
            }
        } catch (Exception e) {
            LOGGER.error("obfuscation failed for workflow field: {} with exception: {}", field, e);
            throw new ObfuscationServiceException("workflow field obfuscation failed", e);
        }
    }

    private void obfuscateTaskField(String field, DocumentContext workflowTasks, List<String> workflowChangedFields) {
        JSONArray task = findTask(buildQuery(getRootField(field)), workflowTasks);
        if(!task.isEmpty()) {
            String fieldToObfuscate = getTaskField(field);
            try {
                DocumentContext parsedTask = JsonPath.parse(task);
                validateTaskField(parsedTask, fieldToObfuscate);
                parsedTask.set(fieldToObfuscate, OBFUSCATION_VALUE);
                workflowChangedFields.add(WORKFLOW_TASKS_FIELD);
            } catch (Exception e) {
                LOGGER.error("obfuscation failed for task field: {} with exception: {}", fieldToObfuscate, e);
                throw new ObfuscationServiceException("workflow task field obfuscation failed", e);
            }
        }
    }

    private JSONArray findTask(String query, DocumentContext workflowTasks) {
        return workflowTasks.read(query);
    }

    private String buildQuery(String referenceTaskName) {
        return String.format(REFERENCE_TASK_NAME_QUERY, referenceTaskName);
    }

    private String getRootField(String obfuscationField) {
        return obfuscationField.split("\\.")[0];
    }

    private String getTaskField(String obfuscationField) {
       return JSON_TASK_INDEX + obfuscationField.replaceFirst(".*?\\.",  "");

    }

    private String convertToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOGGER.error("failed to parse object: {} with error {}", object, e);
            throw new ObfuscationServiceException("parsing object to json failed", e);
        }
    }

    private Workflow getWorkflow(String workflowId) {
        return Optional.ofNullable(executionDAO.getWorkflow(workflowId, true))
                .orElseThrow(() -> new ObfuscationServiceException("workflow not found"));
    }

    private boolean hasFieldsToObfuscate(WorkflowDef workflowDef) {
        return workflowDef.getObfuscationFields() != null && !workflowDef.getObfuscationFields().isEmpty();
    }

    private void validateTaskField(DocumentContext task, String field) {
        task.read(field);
    }

    private List<String> getObfuscationFieldsList(WorkflowDef workflowDef, String entityFields) {
        return asList(workflowDef.getObfuscationFields().get(entityFields).split(","));
    }

    private List<Task> convertJsonTasksToTaskList(DocumentContext jsonWorkflowTasks) {
        try {
            return asList(objectMapper.readValue(jsonWorkflowTasks.jsonString(), Task[].class));
        } catch (Exception e) {
            LOGGER.error("failed to convert task list with error: {}", e);
            throw new ObfuscationServiceException("failed to convert task list", e);
        }
    }
}

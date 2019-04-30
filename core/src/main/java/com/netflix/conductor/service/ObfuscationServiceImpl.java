package com.netflix.conductor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.exception.ObfuscationServiceException;
import net.minidev.json.JSONArray;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;

@Singleton
public class ObfuscationServiceImpl implements ObfuscationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObfuscationServiceImpl.class);
    private static final String REFERENCE_TASK_NAME_QUERY = "$.[?(@.referenceTaskName == '%s')]";
    private static final String JSON_TASK_INDEX = "[0].";
    private static final String WORKFLOW_OBFUSCATION_FIELDS = "workflowFields";
    private static final String TASKS_OBFUSCATION_FIELDS = "taskFields";
    private final ObjectMapper objectMapper;
    private final ExecutionDAO executionDAO;
    private final IndexDAO indexDAO;
    private final MetadataService metadataService;
    private final String valueToReplace;

    @Inject
    public ObfuscationServiceImpl(ObjectMapper objectMapper, ExecutionDAO executionDAO,
                                  IndexDAO indexDAO, MetadataService metadataService, Configuration config) {
        this.objectMapper = objectMapper;
        this.executionDAO = executionDAO;
        this.indexDAO = indexDAO;
        this.metadataService = metadataService;
        this.valueToReplace = config.getProperty("workflow.obfuscation.field.replace.value", "***");
    }

    @Override
    public void obfuscateFields(String workflowId) {
        Workflow workflow = getWorkflow(workflowId);
        WorkflowDef workflowDef = getUpdatedWorkflowDef(workflow.getWorkflowName(), workflow.getWorkflowVersion());

        if(hasFieldsToObfuscate(workflowDef)) {
            DocumentContext jsonWorkflow = JsonPath.parse(convertToJson(workflow));
            DocumentContext jsonWorkflowTasks = JsonPath.parse(convertToJson(workflow.getTasks()));

            List<String> workflowChangedFields = new ArrayList<>();
            List<String> tasksChangedFields = new ArrayList<>();

            getObfuscationFieldsList(workflowDef, WORKFLOW_OBFUSCATION_FIELDS)
                    .forEach(field -> {
                        String obfuscatedField = obfuscateWorkflowField(field, jsonWorkflow);
                        addFieldIfPresent(obfuscatedField, workflowChangedFields);
                    });


            getObfuscationFieldsList(workflowDef, TASKS_OBFUSCATION_FIELDS)
                    .forEach(field -> {
                        String obfuscatedField = obfuscateTaskField(field, jsonWorkflowTasks);
                        addFieldIfPresent(obfuscatedField, tasksChangedFields);
                    });

            if(!workflowChangedFields.isEmpty() || !tasksChangedFields.isEmpty()) {
                if(!tasksChangedFields.isEmpty()) {
                    List<Task> updatedTasks = convertJsonTasksToTaskList(jsonWorkflowTasks);
                    executionDAO.updateTasks(updatedTasks);
                }
                Workflow updatedWorkflow = objectMapper.convertValue(jsonWorkflow.json(), Workflow.class);
                executionDAO.updateWorkflow(updatedWorkflow);
                indexDAO.indexWorkflow(updatedWorkflow);
            }
        }
    }

    private void addFieldIfPresent(String field, List<String> fieldList) {
        if(StringUtils.isNotEmpty(field)) {
            fieldList.add(field);
        }
    }

    private String obfuscateWorkflowField(String field, DocumentContext workflow) {
        try{
            LinkedHashMap fieldToObfuscate = workflow.read(field);
            if(!fieldToObfuscate.isEmpty()) {
                workflow.set(field, valueToReplace);
            }
            return field;
        } catch (Exception e) {
            LOGGER.error("obfuscation failed for workflow field: {} with exception: {}", field, e);
        }
        return null;
    }

    private String obfuscateTaskField(String field, DocumentContext workflowTasks) {
        JSONArray task = findTask(buildQuery(getRootField(field)), workflowTasks);
        if(!task.isEmpty()) {
            String fieldToObfuscate = getTaskField(field);
            try {
                DocumentContext parsedTask = JsonPath.parse(task);
                validateTaskField(parsedTask, fieldToObfuscate);
                parsedTask.set(fieldToObfuscate, valueToReplace);
                return fieldToObfuscate;
            } catch (Exception e) {
                LOGGER.error("obfuscation failed for task field: {} with exception: {}", fieldToObfuscate, e);
            }
        }
        return null;
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

    private WorkflowDef getUpdatedWorkflowDef(String workflowName, int version) {
        return Optional.ofNullable(metadataService.getWorkflowDef(workflowName, version))
                .orElseThrow(() -> new ObfuscationServiceException("workflowDef not found"));
    }

    private boolean hasFieldsToObfuscate(WorkflowDef workflowDef) {
        return workflowDef.getObfuscationFields() != null && !workflowDef.getObfuscationFields().isEmpty();
    }

    private void validateTaskField(DocumentContext task, String field) {
        task.read(field);
    }

    private List<String> getObfuscationFieldsList(WorkflowDef workflowDef, String entityFields) {
        String fields = workflowDef.getObfuscationFields().get(entityFields);
        if(StringUtils.isNotEmpty(fields)) {
            return asList(fields.split(","));
        }
        return new ArrayList<>();
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

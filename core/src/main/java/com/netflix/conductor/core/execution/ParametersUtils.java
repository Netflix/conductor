/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.run.Workflow;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 *
 *
 *
 */
public class ParametersUtils {

    private ObjectMapper om = new ObjectMapper();

    private TypeReference<Map<String, Object>> map = new TypeReference<Map<String, Object>>() {
    };

    public enum SystemParameters {
        CPEWF_TASK_ID,
        NETFLIX_ENV,
        NETFLIX_STACK
    }

    public ParametersUtils() {

    }

    public Map<String, Object> getTaskInput(Map<String, Object> inputParams, Workflow workflow,
                                            TaskDef taskDefinition, String taskId) {
        if (workflow.getSchemaVersion() > 1) {
            return getTaskInputV2(inputParams, workflow, taskId, taskDefinition);
        }
        return getTaskInputV1(workflow, inputParams);
    }

    public Map<String, Object> getTaskInputV2(Map<String, Object> input, Workflow workflow,
                                              String taskId, TaskDef taskDefinition) {
        Map<String, Object> inputParams;

        if (input != null) {
            inputParams = clone(input);
        } else {
            inputParams = new HashMap<>();
        }
        if (taskDefinition != null && taskDefinition.getInputTemplate() != null) {
            inputParams.putAll(clone(taskDefinition.getInputTemplate()));
        }

        Map<String, Map<String, Object>> inputMap = new HashMap<>();

        Map<String, Object> wf = new HashMap<>();
        wf.put("input", workflow.getInput());
        wf.put("output", workflow.getOutput());
        wf.put("status", workflow.getStatus());
        wf.put("workflowId", workflow.getWorkflowId());
        wf.put("parentWorkflowId", workflow.getParentWorkflowId());
        wf.put("parentWorkflowTaskId", workflow.getParentWorkflowTaskId());
        wf.put("workflowType", workflow.getWorkflowType());
        wf.put("version", workflow.getVersion());
        wf.put("correlationId", workflow.getCorrelationId());
        wf.put("reasonForIncompletion", workflow.getReasonForIncompletion());
        wf.put("schemaVersion", workflow.getSchemaVersion());

        inputMap.put("workflow", wf);
        //For new work flow being started the list of tasks will be empty
        workflow.getTasks().stream()
                .map(Task::getReferenceTaskName)
                .map(taskRefName -> workflow.getTaskByRefName(taskRefName))
                .forEach(task -> {
                    Map<String, Object> taskIO = new HashMap<>();
                    taskIO.put("input", task.getInputData());
                    taskIO.put("output", task.getOutputData());
                    taskIO.put("taskType", task.getTaskType());
                    if (task.getStatus() != null) {
                        taskIO.put("status", task.getStatus().toString());
                    }
                    taskIO.put("referenceTaskName", task.getReferenceTaskName());
                    taskIO.put("retryCount", task.getRetryCount());
                    taskIO.put("correlationId", task.getCorrelationId());
                    taskIO.put("pollCount", task.getPollCount());
                    taskIO.put("taskDefName", task.getTaskDefName());
                    taskIO.put("scheduledTime", task.getScheduledTime());
                    taskIO.put("startTime", task.getStartTime());
                    taskIO.put("endTime", task.getEndTime());
                    taskIO.put("workflowInstanceId", task.getWorkflowInstanceId());
                    taskIO.put("taskId", task.getTaskId());
                    taskIO.put("reasonForIncompletion", task.getReasonForIncompletion());
                    taskIO.put("callbackAfterSeconds", task.getCallbackAfterSeconds());
                    taskIO.put("workerId", task.getWorkerId());
                    inputMap.put(task.getReferenceTaskName(), taskIO);
                });

        Configuration option = Configuration.defaultConfiguration()
                .addOptions(Option.SUPPRESS_EXCEPTIONS);
        DocumentContext documentContext = JsonPath.parse(inputMap, option);
        Map<String, Object> replaced = replace(inputParams, documentContext, taskId);
        return replaced;
    }

    //deep clone using json - POJO
    private Map<String, Object> clone(Map<String, Object> inputTemplate) {
        try {

            byte[] bytes = om.writeValueAsBytes(inputTemplate);
            Map<String, Object> cloned = om.readValue(bytes, map);
            return cloned;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Map<String, Object> replace(Map<String, Object> input, Object json) {
        Object doc = null;
        if (json instanceof String) {
            doc = JsonPath.parse(json.toString());
        } else {
            doc = json;
        }
        Configuration option = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
        DocumentContext documentContext = JsonPath.parse(doc, option);
        return replace(input, documentContext, null);
    }

    public Object replace(String paramString) {
        Configuration option = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
        DocumentContext documentContext = JsonPath.parse(Collections.emptyMap(), option);
        return replaceVariables(paramString, documentContext, null);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> replace(Map<String, Object> input, DocumentContext documentContext, String taskId) {
        for (Entry<String, Object> e : input.entrySet()) {
            Object value = e.getValue();
            if (value instanceof String || value instanceof Number) {
                Object replaced = replaceVariables(value.toString(), documentContext, taskId);
                e.setValue(replaced);
            } else if (value instanceof Map) {
                //recursive call
                Object replaced = replace((Map<String, Object>) value, documentContext, taskId);
                e.setValue(replaced);
            } else if (value instanceof List) {
                Object replaced = replaceList((List<?>) value, taskId, documentContext);
                e.setValue(replaced);
            } else {
                e.setValue(value);
            }
        }
        return input;
    }

    @SuppressWarnings("unchecked")
    private Object replaceList(List<?> values, String taskId, DocumentContext io) {
        List<Object> replacedList = new LinkedList<>();
        for (Object listVal : values) {
            if (listVal instanceof String) {
                Object replaced = replaceVariables(listVal.toString(), io, taskId);
                replacedList.add(replaced);
            } else if (listVal instanceof Map) {
                Object replaced = replace((Map<String, Object>) listVal, io, taskId);
                replacedList.add(replaced);
            } else if (listVal instanceof List) {
                Object replaced = replaceList((List<?>) listVal, taskId, io);
                replacedList.add(replaced);
            } else {
                replacedList.add(listVal);
            }
        }
        return replacedList;
    }

    private Object replaceVariables(String paramString, DocumentContext documentContext, String taskId) {
        String[] values = paramString.split("(?=\\$\\{)|(?<=\\})");
        Object[] convertedValues = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            convertedValues[i] = values[i];
            if (values[i].startsWith("${") && values[i].endsWith("}")) {
                String paramPath = values[i].substring(2, values[i].length() - 1);
                if (contains(paramPath)) {
                    String sysValue = getSystemParametersValue(paramPath, taskId);
                    if (sysValue != null) {
                        convertedValues[i] = sysValue;
                    }

                } else {
                    convertedValues[i] = documentContext.read(paramPath);
                }

            }
        }

        Object retObj = convertedValues[0];
        // If the parameter String was "v1 v2 v3" then make sure to stitch it back
        if (convertedValues.length > 1) {
            for (int i = 0; i < convertedValues.length; i++) {
                Object val = convertedValues[i];
                if (val == null) {
                    val = "";
                }
                if (i == 0) {
                    retObj = val;
                } else {
                    retObj = retObj + "" + val.toString();
                }
            }

        }
        return retObj;
    }

    private String getSystemParametersValue(String sysParam, String taskId) {
        if ("CPEWF_TASK_ID".equals(sysParam)) {
            return taskId;
        }
        String value = System.getenv(sysParam);
        if (value == null) {
            value = System.getProperty(sysParam);
        }
        return value;
    }

    private boolean contains(String test) {
        for (SystemParameters c : SystemParameters.values()) {
            if (c.name().equals(test)) {
                return true;
            }
        }
        String value = Optional.ofNullable(System.getProperty(test)).orElse(Optional.ofNullable(System.getenv(test)).orElse(null));
        return value != null;
    }

    @Deprecated
    //Workflow schema version 1 is deprecated and new workflows should be using version 2
    private Map<String, Object> getTaskInputV1(Workflow workflow, Map<String, Object> inputParams) {
        Map<String, Object> input = new HashMap<>();
        if (inputParams == null) {
            return input;
        }
        Map<String, Object> workflowInput = workflow.getInput();
        inputParams.entrySet().forEach(e -> {

            String paramName = e.getKey();
            String paramPath = "" + e.getValue();
            String[] paramPathComponents = paramPath.split("\\.");
            Preconditions.checkArgument(paramPathComponents.length == 3, "Invalid input expression for " + paramName + ", paramPathComponents.size=" + paramPathComponents.length + ", expression=" + paramPath);

            String source = paramPathComponents[0];    //workflow, or task reference name
            String type = paramPathComponents[1];    //input/output
            String name = paramPathComponents[2];    //name of the parameter
            if ("workflow".equals(source)) {
                input.put(paramName, workflowInput.get(name));
            } else {
                Task task = workflow.getTaskByRefName(source);
                if (task != null) {
                    if ("input".equals(type)) {
                        input.put(paramName, task.getInputData().get(name));
                    } else {
                        input.put(paramName, task.getOutputData().get(name));
                    }
                }
            }
        });
        return input;
    }


}

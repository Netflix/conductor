/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.ScriptEvaluator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * 
 * 
 *
 */
public class ParametersUtils {
	private static Logger logger = LoggerFactory.getLogger(ParametersUtils.class);
	
	private ObjectMapper om = new ObjectMapper();
	
	private TypeReference<Map<String, Object>> map = new TypeReference<Map<String,Object>>() {};

	public enum SystemParameters {
		UUID,
		CPEWF_TASK_ID,
		NETFLIX_ENV,
		NETFLIX_STACK,
		CPEWF_CURRENT_TIMESTAMP
	}
	
	public ParametersUtils() {

	}

	public Map<String, Object> getTaskInputV2(Map<String, Object> input, Workflow workflow, String taskId, TaskDef taskDef) {
		return getTaskInputV2(input, null, workflow, taskId, taskDef, null);
	}

	public Map<String, Object> getTaskInputV2(Map<String, Object> input, Workflow workflow, String taskId, TaskDef taskDef, WorkflowTask workflowTask) {
		return getTaskInputV2(input, null, workflow, taskId, taskDef, workflowTask);
	}

	public Map<String, Object> getTaskInputV2(Map<String, Object> input, Map<String, Map<String, Object>> defaults, Workflow workflow, String taskId, TaskDef taskDef, WorkflowTask workflowTask) {
		Map<String, Object> inputParams = null;
		if(input != null) {
			inputParams = clone(input);
		} else {
			inputParams = new HashMap<>();
		}
		if(taskDef != null && taskDef.getInputTemplate() != null) {
			inputParams.putAll(clone(taskDef.getInputTemplate()));
		}

		Map<String, Map<String, Object>> inputMap = getInputMap(defaults, workflow, taskId, workflowTask);

		Configuration option = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
		DocumentContext io = JsonPath.parse(inputMap, option);
		return replace(inputParams, io, taskId);
	}

	public Map<String, Map<String, Object>> getInputMap(Map<String, Map<String, Object>> defaults, Workflow workflow, String taskId, WorkflowTask workflowTask) {
		Map<String, Map<String, Object>> inputMap = new HashMap<>();

		DateTimeFormatter fmt = ISODateTimeFormat.dateTime().withZoneUTC();
		Map<String, Object> wf = new HashMap<>();
		wf.put("input", workflow.getInput());
		wf.put("output", workflow.getOutput());
		wf.put("status", workflow.getStatus());
		wf.put("startTime", workflow.getStartTime());
		wf.put("startTimeIso", fmt.print(workflow.getStartTime()));
		wf.put("workflowId", workflow.getWorkflowId());
		wf.put("parentWorkflowId", workflow.getParentWorkflowId());
		wf.put("parentWorkflowTaskId", workflow.getParentWorkflowTaskId());
		wf.put("workflowType", workflow.getWorkflowType());
		wf.put("version", workflow.getVersion());
		wf.put("correlationId", workflow.getCorrelationId());
		wf.put("reasonForIncompletion", workflow.getReasonForIncompletion());
		wf.put("schemaVersion", workflow.getSchemaVersion());
		wf.put("workflowIds", workflow.getWorkflowIds());
		wf.put("authorization", workflow.getAuthorization());
		wf.put("contextUser", workflow.getContextUser());
		wf.put("tags", workflow.getTags());
		wf.put("traceId", workflow.getTraceId());
		wf.put("jobPriority", workflow.getJobPriority());

		String correlationId = workflow.getCorrelationId();
		if (StringUtils.isNotEmpty(correlationId) && correlationId.startsWith("{") && correlationId.endsWith("}")) {
			try {
				Map<String, Object> cloned = om.readValue(correlationId, map);
				wf.put("correlationMap", cloned);
			} catch (IOException e) {
				logger.error("Unable to parse workflow correlation id");
			}
		}

		inputMap.put("workflow", wf);
		if (MapUtils.isNotEmpty(defaults)) {
			inputMap.putAll(defaults);
		}

		if (workflowTask != null) {
			Map<String, Object> taskIO = new HashMap<>();
			taskIO.put("referenceTaskName", workflowTask.getTaskReferenceName());
			inputMap.put("task", taskIO);
		}

		if (taskId != null) {
			Optional<Task> task = workflow.getTasks().stream()
					.filter(item -> item.getTaskId().equalsIgnoreCase(taskId)).findFirst();
			if (task.isPresent()) {
				Map<String, Object> taskIO = createTaskIO(task.get());
				inputMap.put("task", taskIO);
			}
		}

		workflow.getTasks().stream().map(Task::getReferenceTaskName).map(taskRefName -> workflow.getTaskByRefName(taskRefName)).forEach(task -> {
			Map<String, Object> taskIO = createTaskIO(task);
			inputMap.put(task.getReferenceTaskName(), taskIO);
		});

		return inputMap;
	}

	private Map<String, Object> createTaskIO(Task task) {
		Map<String, Object> taskIO = new HashMap<>();
		taskIO.put("input", task.getInputData());
		taskIO.put("output", task.getOutputData());
		taskIO.put("taskType", task.getTaskType());
		if(task.getStatus() != null) {
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
		return taskIO;
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
		if(json instanceof String) {
			doc = JsonPath.parse(json.toString());
		} else {
			doc = json;
		}
		Configuration option = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
		DocumentContext io = JsonPath.parse(doc, option);
		return replace(input, io, null);
	}
	
	public Object replace(String paramString) {
		Configuration option = Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);
		DocumentContext io = JsonPath.parse(Collections.emptyMap(), option);
		return replaceVariables(paramString, io, null);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> replace(Map<String, Object> input, DocumentContext io, String taskId) {
		for (Entry<String, Object> e : input.entrySet()) {
			Object value = e.getValue();
			if (value instanceof String) {
				Object replaced = replaceVariables(value.toString(), io, taskId);
				e.setValue(replaced);
			} else if (value instanceof Map) {
				Object replaced = replace((Map<String, Object>) value, io, taskId);
				e.setValue(replaced);
			} else if (value instanceof List) {
				Object replaced = replaceList((List<?>) value, taskId, io);
				e.setValue(replaced);
			}else {
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
			}else {
				replacedList.add(listVal);
			}
		}
		return replacedList;
	}

	private Object replaceVariables(String paramString, DocumentContext io, String taskId){
		String[] values = paramString.split("(?=\\$\\{)|(?=\\$jq\\{)|(?<=\\})");
		Object[] convertedValues = new Object[values.length];
		for(int i=0; i < values.length; i++){
			convertedValues[i] = values[i];
			if(values[i].startsWith("${") && values[i].endsWith("}")){
				String paramPath = values[i].substring(2, values[i].length()-1);
				if (contains(paramPath)) {
					String sysValue = getSystemParametersValue(paramPath, taskId);
					if(sysValue != null){
						convertedValues[i] = sysValue;
					}

				} else {
					convertedValues[i] = io.read(paramPath);
				}
			} else if(values[i].startsWith("$jq{") && values[i].endsWith("}")) {
				String expression = values[i].substring(4, values[i].length() - 1);
				try {
					Object json = io.read("$");
					List<Object> result = ScriptEvaluator.evalJqAsList(expression, json);
					convertedValues[i] = (CollectionUtils.isNotEmpty(result) ? result.get(0) : null);
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		}

		Object retObj = convertedValues[0];
		// If the parameter String was "v1 v2 v3" then make sure to stitch it back
		if(convertedValues.length > 1){
			for (int i = 0; i < convertedValues.length; i++) {
				Object val = convertedValues[i];
				if(val == null){
					val = "";
				}
				if(i == 0){
					retObj = val;
				} else {
					retObj = retObj + "" + val.toString();				
				}
			}
			
		}
		return retObj;
	}

	private String getSystemParametersValue(String sysParam, String taskId){
		if("UUID".equals(sysParam)) {
			return UUID.randomUUID().toString();
		} else if("CPEWF_TASK_ID".equals(sysParam)) {
			return taskId;
		} else if (sysParam.startsWith("CPEWF_CURRENT_TIMESTAMP")) {
			try {
				if (sysParam.contains(":")) {
					String format = sysParam.substring(sysParam.indexOf(":") + 1);
					SimpleDateFormat fmt = new SimpleDateFormat(format);
					return fmt.format(new Date());
				} else {
					DateTimeFormatter fmt = ISODateTimeFormat.dateTime().withZoneUTC();
					return fmt.print(new DateTime());
				}
			} catch (Exception ex) {
				logger.error("Unable to get param value for '" + sysParam + "'", ex);
				return null;
			}
		}

		String value = System.getenv(sysParam);
		if(value == null) {
			value = System.getProperty(sysParam);
		}
		return value;
	}
	
	private boolean contains(String test) {
		for (SystemParameters c : SystemParameters.values()) {
			if (test.startsWith(c.name())) {
				return true;
			}
		}
		String value = Optional.ofNullable(System.getProperty(test)).orElse(Optional.ofNullable(System.getenv(test)).orElse(null));
		return value != null;
	}
	
}

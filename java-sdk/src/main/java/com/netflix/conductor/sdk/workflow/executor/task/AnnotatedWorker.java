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
package com.netflix.conductor.sdk.workflow.executor.task;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.sdk.task.InputParam;
import com.netflix.conductor.sdk.task.OutputParam;
import com.netflix.conductor.sdk.workflow.utils.ObjectMapperProvider;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AnnotatedWorker implements Worker {

    private String name;

    private Method workerMethod;

    private Object obj;

    private ObjectMapper om = new ObjectMapperProvider().getObjectMapper();

    private int pollingInterval = 100;

    public AnnotatedWorker(String name, Method workerMethod, Object obj) {
        this.name = name;
        this.workerMethod = workerMethod;
        this.obj = obj;
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public String getTaskDefName() {
        return name;
    }

    @Override
    public TaskResult execute(Task task) {
        TaskResult result = new TaskResult(task);
        try {
            Object[] parameters = getInvocationParameters(task);
            Object invocationResult = workerMethod.invoke(obj, parameters);
            result = setValue(invocationResult, task);
        } catch (Exception e) {
            e.printStackTrace();
            Throwable rootCause = e.getCause();
            result.setStatus(TaskResult.Status.FAILED);
            result.setReasonForIncompletion(rootCause.getMessage());
        }
        return result;
    }

    private Object[] getInvocationParameters(Task task) {
        Class<?>[] parameterTypes = workerMethod.getParameterTypes();

        if (parameterTypes.length == 1 && parameterTypes[0].equals(Task.class)) {
            return new Object[] {task};
        } else if (parameterTypes.length == 1 && parameterTypes[0].equals(Map.class)) {
            return new Object[] {task.getInputData()};
        }

        Annotation[][] parameterAnnotations = workerMethod.getParameterAnnotations();
        Object[] values = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Annotation[] paramAnnotation = parameterAnnotations[i];
            if (paramAnnotation != null && paramAnnotation.length > 0) {
                for (Annotation ann : paramAnnotation) {
                    if (ann.annotationType().equals(InputParam.class)) {
                        InputParam ip = (InputParam) ann;
                        String name = ip.value();
                        Object value = task.getInputData().get(name);
                        values[i] = om.convertValue(value, parameterTypes[0]);
                    }
                }
            } else {
                Object input = om.convertValue(task.getInputData(), parameterTypes[0]);
                values[i] = input;
            }
        }
        return values;
    }

    private TaskResult setValue(Object invocationResult, Task task) {

        if (invocationResult == null) {
            task.setStatus(Task.Status.COMPLETED);
            return new TaskResult(task);
        }

        OutputParam opAnnotation =
                workerMethod.getAnnotatedReturnType().getAnnotation(OutputParam.class);
        if (opAnnotation != null) {

            String name = opAnnotation.value();
            task.getOutputData().put(name, invocationResult);
            task.setStatus(Task.Status.COMPLETED);
            return new TaskResult(task);

        } else if (invocationResult instanceof TaskResult) {

            return (TaskResult) invocationResult;

        } else if (invocationResult instanceof Map) {
            Map resultAsMap = (Map) invocationResult;
            task.getOutputData().putAll(resultAsMap);
            task.setStatus(Task.Status.COMPLETED);
            return new TaskResult(task);
        } else if (invocationResult instanceof String
                || invocationResult instanceof Number
                || invocationResult instanceof Boolean) {
            task.getOutputData().put("result", invocationResult);
            task.setStatus(Task.Status.COMPLETED);
            return new TaskResult(task);
        } else if (invocationResult instanceof List) {

            List resultAsList = om.convertValue(invocationResult, List.class);
            task.getOutputData().put("result", resultAsList);
            task.setStatus(Task.Status.COMPLETED);
            return new TaskResult(task);

        } else {
            Map resultAsMap = om.convertValue(invocationResult, Map.class);
            task.getOutputData().putAll(resultAsMap);
            task.setStatus(Task.Status.COMPLETED);
            return new TaskResult(task);
        }
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    @Override
    public int getPollingInterval() {
        return pollingInterval;
    }
}

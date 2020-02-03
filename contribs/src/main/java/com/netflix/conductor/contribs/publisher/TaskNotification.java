package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

@JsonNaming(PropertyNamingStrategy.UpperCamelCaseStrategy.class)
class TaskNotification {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisherX.class);
    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
    public String taskId;
    public Task.Status taskStatus;
    public String taskDescription;
    public String taskType;
    public String taskDefName;
    public String referenceTaskName;
    public String taskInput;
    public String taskOutput;
    public String taskScheduledTime;
    public String taskStartTime;
    public String taskUpdateTime;
    public String taskEndTime;
    public String taskQueueWaitTime;
    public String reasonForIncompletion;
    public int retryCount;
    public boolean isExecuted;
    public String workflowId;
    public String workflowType;
    public String correlationId;

    TaskNotification(Task task) throws JsonProcessingException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(gmt);

        taskId = task.getTaskId();
        taskStatus = task.getStatus();
        taskDescription = task.getTaskDescription();
        taskType = task.getTaskType();
        taskDefName = task.getTaskDefName();
        referenceTaskName = task.getReferenceTaskName();
        taskInput = new ObjectMapper().writeValueAsString(task.getInputData());
        taskOutput = new ObjectMapper().writeValueAsString(task.getInputData());
        taskScheduledTime = sdf.format(new Date(task.getScheduledTime()));
        taskStartTime = sdf.format(new Date(task.getStartTime()));
        taskUpdateTime = String.valueOf(task.getUpdateTime());
        taskEndTime = sdf.format(new Date(task.getUpdateTime()));
        taskQueueWaitTime = sdf.format(new Date(task.getEndTime()));
        reasonForIncompletion = task.getReasonForIncompletion();
        retryCount = task.getRetryCount();
        isExecuted = task.isExecuted();
        workflowId = task.getWorkflowInstanceId();
        workflowType = task.getWorkflowType();
        correlationId = task.getCorrelationId();
    }

    String toJsonString() {
        String jsonString;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            jsonString = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to convert Task: {} to String. Exception: {}", this, e);
            throw new RuntimeException(e);
        }
        return jsonString;
    }
}

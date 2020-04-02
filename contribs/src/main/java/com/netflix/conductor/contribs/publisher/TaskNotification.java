package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.TaskSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TimeZone;

//@JsonNaming(PropertyNamingStrategy.UpperCamelCaseStrategy.class)
public class TaskNotification extends TaskSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisher.class);
    private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
    public String queueWaitTime;
    public String workflowTaskType;
    private String domainGroupMoId;
    private String accountMoId;

    public String getDomainGroupMoId() {
        return domainGroupMoId;
    }
    public String getAccountMoId() {
        return accountMoId;
    }


    /*private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
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
    public String workflowTaskType;
    private String domainGroupMoId;
    private String accountMoId;

    public String getDomainGroupMoId() {
        return domainGroupMoId;
    }

    public String getAccountMoId() {
        return accountMoId;
    }*/

    TaskNotification(Task task) {
        super(task);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(gmt);

        /*
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
        workflowTaskType = task.getWorkflowTask().getType();
         */

        queueWaitTime = sdf.format(new Date(task.getQueueWaitTime()));
        workflowTaskType = task.getWorkflowTask().getType();
        domainGroupMoId = ((LinkedHashMap) task.getInputData().get("FusionMeta")).get("DomainGroupMoId").toString();
        accountMoId = ((LinkedHashMap) task.getInputData().get("FusionMeta")).get("AccountMoId").toString();
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

package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.TaskSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.LinkedHashMap;

public class TaskNotification extends TaskSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisher.class);

    public String workflowTaskType;
    private String domainGroupMoId;
    private String accountMoId;

    public String getDomainGroupMoId() {
        return domainGroupMoId;
    }
    public String getAccountMoId() {
        return accountMoId;
    }

    TaskNotification(Task task) {
        super(task);
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

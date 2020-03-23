package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

public class WorkflowNotification extends WorkflowSummary {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusPublisher.class);

    private String domainGroupMoId;
    private String accountMoId;


    public String getDomainGroupMoId() {
        return domainGroupMoId;
    }
    public String getAccountMoId() {
        return accountMoId;
    }

    public WorkflowNotification(Workflow workflow) {
        super(workflow);
        domainGroupMoId = ((LinkedHashMap) workflow.getInput().get("FusionMeta")).get("DomainGroupMoId").toString();
        accountMoId = ((LinkedHashMap) workflow.getInput().get("FusionMeta")).get("AccountMoId").toString();
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

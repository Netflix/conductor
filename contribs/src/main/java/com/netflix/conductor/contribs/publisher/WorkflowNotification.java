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
    private String domainGroupMoId = "";
    private String accountMoId = "";

    public String getDomainGroupMoId() {
        return domainGroupMoId;
    }
    public String getAccountMoId() {
        return accountMoId;
    }

    public WorkflowNotification(Workflow workflow) {
        super(workflow);

        boolean isFusionMetaPresent =  workflow.getInput().containsKey("FusionMeta");
        if (!isFusionMetaPresent) {
            return;
        }

        LinkedHashMap fusionMeta = (LinkedHashMap) workflow.getInput().get("FusionMeta");
        domainGroupMoId = fusionMeta.containsKey("DomainGroupMoId") ? fusionMeta.get("DomainGroupMoId").toString(): "";
        accountMoId = fusionMeta.containsKey("AccountMoId") ? fusionMeta.get("AccountMoId").toString(): "";
    }

    String toJsonString() {
        String jsonString;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            jsonString = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to convert workflow: {} to String. Exception: {}", this, e);
            throw new RuntimeException(e);
        }
        return jsonString;
    }
}

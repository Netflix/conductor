package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

class WorkflowNotification extends WorkflowSummary {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusPublisher.class);
    private String domainGroupMoId = "";
    private String accountMoId = "";

    String getDomainGroupMoId() {
        return domainGroupMoId;
    }
    String getAccountMoId() {
        return accountMoId;
    }

    WorkflowNotification(Workflow workflow) {
        super(workflow);

        boolean isFusionMetaPresent =  workflow.getInput().containsKey("_ioMeta");
        if (!isFusionMetaPresent) {
            return;
        }

        LinkedHashMap fusionMeta = (LinkedHashMap) workflow.getInput().get("_ioMeta");
        domainGroupMoId = fusionMeta.containsKey("DomainGroupMoId") ? fusionMeta.get("DomainGroupMoId").toString(): "";
        accountMoId = fusionMeta.containsKey("AccountMoId") ? fusionMeta.get("AccountMoId").toString(): "";
    }

    String toJsonString() {
        String jsonString;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            jsonString = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to convert workflow {} id: {} to String. Exception: {}", this.getWorkflowType(), this.getWorkflowId(), e);
            throw new RuntimeException(e);
        }
        return jsonString;
    }
}

package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowStatusPublisher implements WorkflowStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusPublisher.class);
    public static final String NOTIFICATION_TYPE = "WorkflowNotification";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        LOGGER.info("#### Publishing workflow {} on completion callback", workflow.getWorkflowId());
        //publishWorkflow(workflow);
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        LOGGER.info("#### Publishing workflow {} on termination callback", workflow.getWorkflowId());
        //publishWorkflow(workflow);
    }

    private String workflowToMessage(Workflow workflow) {
        String jsonWf;
        try {
            jsonWf = objectMapper.writeValueAsString(workflow);
        } catch (JsonProcessingException e) {
            LOGGER.error("#### Failed to convert Workflow: {} to String. Exception: {}", workflow, e);
            throw new RuntimeException(e);
        }
        return jsonWf;
    }

    private void publishWorkflow(Workflow workflow) {
        String jsonWf = workflowToMessage(workflow);

        RestClient rc = new RestClient();
        String url = rc.createUrl(NOTIFICATION_TYPE);
        rc.post(url, jsonWf, "domainGroupMoId", "AccountMoId");
    }

    public void publish(Workflow workflow) {
        publishWorkflow(workflow);
    }
}
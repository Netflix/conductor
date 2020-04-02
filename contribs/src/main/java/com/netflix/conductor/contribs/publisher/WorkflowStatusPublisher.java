package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class WorkflowStatusPublisher implements WorkflowStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusPublisher.class);
    public static final String NOTIFICATION_TYPE = "workflow/WorkflowNotifications";
    private static final Integer QDEPTH = 10;
    private BlockingQueue<Workflow> blockingQueue = new LinkedBlockingDeque<>(QDEPTH);
    private boolean isConsumerRunning = false;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Thread consumerThread = new Thread(() -> {
        try {
            while (true) {
                Workflow workflow = blockingQueue.take();
                LOGGER.info("Consume " + workflow);
                WorkflowNotification workflowNotification = new WorkflowNotification(workflow);
                publishWorkflowNotification(workflowNotification);
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (Exception e){
            LOGGER.info(e.getMessage());
        }
    });

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        try {
            LOGGER.info("#### Publishing workflow {} on completion callback", workflow.getWorkflowId());
            blockingQueue.put(workflow);

            // start if consumer thread not running
            if (isConsumerRunning) {
                return;
            }
            consumerThread.start();
            isConsumerRunning = true;
        } catch (Exception e){
            LOGGER.info(e.getMessage());
        }
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {

        try {
            LOGGER.info("#### Publishing workflow {} on termination callback", workflow.getWorkflowId());
            blockingQueue.put(workflow);

            // start if consumer thread not running
            if (isConsumerRunning) {
                return;
            }
            consumerThread.start();
            isConsumerRunning = true;
        } catch (Exception e){
            LOGGER.info(e.getMessage());
        }
    }

    private void publishWorkflowNotification(WorkflowNotification workflowNotification) {
        String jsonWorkflow = workflowNotification.toJsonString();
        LOGGER.info("##### Task Message {} .", jsonWorkflow);
        RestClient rc = new RestClient();
        String url = rc.createUrl(NOTIFICATION_TYPE);
        rc.post(url, jsonWorkflow, workflowNotification.getDomainGroupMoId(), workflowNotification.getAccountMoId());
    }
}
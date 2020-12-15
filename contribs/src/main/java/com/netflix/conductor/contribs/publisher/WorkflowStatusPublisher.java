package com.netflix.conductor.contribs.publisher;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Singleton
public class WorkflowStatusPublisher implements WorkflowStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusPublisher.class);
    private static final String NOTIFICATION_TYPE = "workflow/WorkflowNotifications";
    private static final Integer QDEPTH = Integer.parseInt(System.getenv("ENV_WORKFLOW_NOTIFICATION_QUEUE_SIZE"));
    private BlockingQueue<Workflow> blockingQueue = new LinkedBlockingDeque<>(QDEPTH);

    class ExceptionHandler implements Thread.UncaughtExceptionHandler
    {
        public void uncaughtException(Thread t, Throwable e)
        {
            LOGGER.info("An exception has been captured\n");
            LOGGER.info("Thread: {}\n", t.getName());
            LOGGER.info("Exception: {}: {}\n", e.getClass().getName(), e.getMessage());
//            LOGGER.info("Stack Trace: \n");
//            e.printStackTrace(System.out);
//            LOGGER.info("Thread status: {}\n", t.getState());
            new ConsumerThread().start();
        }
    }

    class ConsumerThread extends Thread {

        public void run(){
            this.setUncaughtExceptionHandler(new ExceptionHandler());
            String tName = Thread.currentThread().getName();
            LOGGER.info("{}: Starting consumer thread", tName);

            while (true) {
                try {
                    Workflow workflow = blockingQueue.take();
                    WorkflowNotification workflowNotification = new WorkflowNotification(workflow);
                    String jsonWorkflow = workflowNotification.toJsonString();
                    LOGGER.info("Publishing WorkflowNotification: {}", jsonWorkflow);
                    if (workflowNotification.getAccountMoId().equals("")) {
                        LOGGER.info("Skip workflow '{}' notification. Account Id is empty.", workflowNotification.getWorkflowId());
                        continue;
                    }
                    if (workflowNotification.getDomainGroupMoId().equals("")) {
                        LOGGER.info("Skip workflow '{}' notification. Domain group is empty.", workflowNotification.getWorkflowId());
                        continue;
                    }
                    publishWorkflowNotification(workflowNotification);
                    LOGGER.debug("Workflow {} publish is successful.", workflowNotification.getWorkflowId());
                    Thread.sleep(5);
                }
                catch (Exception e) {
                    LOGGER.error("Failed to publish workflow: {} to String. Exception: {}", this, e);
                    LOGGER.error(e.getMessage());
                }
            }
        }
    }

    public WorkflowStatusPublisher() {
        ConsumerThread consumerThread = new ConsumerThread();
        consumerThread.start();
    }

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        try {
            blockingQueue.put(workflow);
        } catch (Exception e){
            LOGGER.error("Failed to enqueue workflow: {} to String. Exception: {}", this, e);
            LOGGER.error(e.getMessage());
        }
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        try {
            blockingQueue.put(workflow);
        } catch (Exception e){
            LOGGER.error("Failed to enqueue workflow: {} to String. Exception: {}", this, e);
            LOGGER.error(e.getMessage());
        }
    }

     private void publishWorkflowNotification(WorkflowNotification workflowNotification) {
        String jsonWorkflow = workflowNotification.toJsonString();
        RestClient rc = new RestClient();
        String url = rc.createUrl(NOTIFICATION_TYPE);
        rc.post(url, jsonWorkflow, workflowNotification.getDomainGroupMoId(), workflowNotification.getAccountMoId());
    }
}
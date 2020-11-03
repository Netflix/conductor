package com.netflix.conductor.contribs.publisher;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.execution.TaskStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Singleton
public class TaskStatusPublisher implements TaskStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisher.class);
    private static final String NOTIFICATION_TYPE = "workflow/TaskNotifications";
    private static final Integer QDEPTH = Integer.parseInt(System.getenv("ENV_TASK_NOTIFICATION_QUEUE_SIZE"));
    private BlockingQueue<Task> blockingQueue = new LinkedBlockingDeque<>(QDEPTH);

    class ExceptionHandler implements Thread.UncaughtExceptionHandler
    {
        public void uncaughtException(Thread t, Throwable e)
        {
            LOGGER.info("An exception has been captured\n");
            LOGGER.info("Thread: {}\n", t.getName());
            LOGGER.info("Exception: {}: {}\n", e.getClass().getName(), e.getMessage());
            LOGGER.info("Stack Trace: \n");
            e.printStackTrace(System.out);
            LOGGER.info("Thread status: {}\n", t.getState());
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
                    Task task = blockingQueue.take();
                    TaskNotification taskNotification = new TaskNotification(task);
                    String taskMsg = String.format("Publishing task '%s'. WorkflowId: '%s'.",taskNotification.getTaskId(), taskNotification.getWorkflowId());
                    LOGGER.debug(taskMsg);
                    if (taskNotification.getAccountMoId().equals("")) {
                        LOGGER.info("Skip task '{}' notification. Account Id is empty.", taskNotification.getTaskId());
                        continue;
                    }
                    if (taskNotification.getDomainGroupMoId().equals("")) {
                        LOGGER.info("Skip task '{}' notification. Domain group is empty.", taskNotification.getTaskId());
                        continue;
                    }
                    publishTaskNotification(taskNotification);
                    LOGGER.debug("Task {} publish is successful.", taskNotification.getTaskId());
                    Thread.sleep(5);
                }
                catch (Exception e) {
                    LOGGER.error("Failed to publish task: {} to String. Exception: {}", this, e);
                    LOGGER.error(e.getMessage());
                }
            }
        }
    }

    public TaskStatusPublisher() {
        ConsumerThread consumerThread = new ConsumerThread();
        consumerThread.start();
    }

    @Override
    public void onTaskScheduled(Task task) {
        try {
            blockingQueue.put(task);
        } catch (Exception e){
            LOGGER.error("Failed to enqueue task: {} to String. Exception: {}", this, e);
            LOGGER.error(e.getMessage());
        }
    }

    private void publishTaskNotification(TaskNotification taskNotification) {
        String jsonTask = taskNotification.toJsonString();
        LOGGER.info("Publishing TaskNotification: {}", jsonTask);
        RestClient rc = new RestClient();
        String url = rc.createUrl(NOTIFICATION_TYPE);
        rc.post(url, jsonTask, taskNotification.getDomainGroupMoId(), taskNotification.getAccountMoId());
    }

}
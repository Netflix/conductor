package com.netflix.conductor.contribs.publisher;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.execution.TaskStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TaskStatusPublisher implements TaskStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisher.class);
    private static final String NOTIFICATION_TYPE = "workflow/TaskNotifications";
    private static final Integer QDEPTH = 10;
    private BlockingQueue<Task> blockingQueue = new LinkedBlockingDeque<>(QDEPTH);
    private boolean isConsumerRunning = false;

    private Thread consumerThread = new Thread(() -> {
        try {
            while (true) {
                Task task = blockingQueue.take();
                LOGGER.info("Consume " + task);
                TaskNotification taskNotification = new TaskNotification(task);
                publishTaskNotification(taskNotification);
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
    public void onTaskScheduled(Task task) {
        try {
            LOGGER.info("#### Publishing Task {} on schedule callback", task.getTaskId());
            blockingQueue.put(task);

            // start if consumer thread not running
            if (isConsumerRunning) {
                return;
            }
            consumerThread.start();
            //consumerThread.join();
            isConsumerRunning = true;
        } catch (Exception e){
            LOGGER.info(e.getMessage());
        }
        /*new Thread(() -> {
            try {
                TaskNotification taskNotification = new TaskNotification(task);
                publishTaskNotification(taskNotification);
            } catch (Exception e){
                LOGGER.info(e.getMessage());
            }
        }).start();*/
    }

    private void publishTaskNotification(TaskNotification taskNotification) {
        String jsonTask = taskNotification.toJsonString();
        LOGGER.info("##### Task Message {} .", jsonTask);
        RestClient rc = new RestClient();
        String url = rc.createUrl(NOTIFICATION_TYPE);
        // rc.get(url);
        rc.post(url, jsonTask, taskNotification.getDomainGroupMoId(), taskNotification.getAccountMoId());
    }

}
package com.netflix.conductor.contribs.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.execution.TaskStatusListenerX;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskStatusPublisherX implements TaskStatusListenerX {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusPublisherX.class);
    public static final String NOTIFICATION_TYPE = "workflow/TaskNotifications";

    @Override
    public void onTaskScheduled(Task task) {
        LOGGER.info("#### Publishing Task {} on schedule callback", task.getTaskId());
        try {
            TaskNotification taskNotification = new TaskNotification(task);
            publishTaskNotification(taskNotification);
        } catch (Exception e){
            LOGGER.info(e.getMessage());
        }
    }

    private void publishTaskNotification(TaskNotification taskNotification) {
        String jsonTask = taskNotification.toJsonString();
        LOGGER.info("##### Task Message {} .", jsonTask);
        RestClient rc = new RestClient();
        String url = rc.createUrl(NOTIFICATION_TYPE);
        // rc.get(url);
        rc.post(url, jsonTask);
    }

}
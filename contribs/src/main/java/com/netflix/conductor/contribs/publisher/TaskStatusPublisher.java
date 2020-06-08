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
                    LOGGER.info("{}: Consume {}", tName, task);
                    TaskNotification taskNotification = new TaskNotification(task);
                    if (taskNotification.getAccountMoId().equals("") || taskNotification.getDomainGroupMoId().equals("")) {
                        LOGGER.info("{}: Skip publishing task notification", tName);
                        continue;
                    }
                    publishTaskNotification(taskNotification);
                    Thread.sleep(10);
                }
                catch (Exception e) {
                    LOGGER.error("{}: Failed to consume Task: {} to String. Exception: {}", tName, this, e);
                    LOGGER.info(e.getMessage());
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
            LOGGER.info("#### Publishing Task {} on schedule callback", task.getTaskId());
            blockingQueue.put(task);
        } catch (Exception e){
            LOGGER.error("Error on scheduling task. Exception: {}", this, e);
        }
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
/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.client.automator;

import com.google.common.base.Stopwatch;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.telemetry.MetricsContainer;
import com.netflix.conductor.client.worker.PropertyFactory;
import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.utils.RetryUtil;
import com.netflix.discovery.EurekaClient;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the threadpool used by the workers for execution and server communication (polling and task update).
 */
class TaskPollExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskPollExecutor.class);

    private EurekaClient eurekaClient;
    private TaskClient taskClient;
    private int updateRetryCount;
    private ExecutorService executorService;
    private PollingSemaphore pollingSemaphore;
    private Map<String/*taskType*/, String/*domain*/> taskToDomain;

    private static final String DOMAIN = "domain";
    private static final String ALL_WORKERS = "all";

    TaskPollExecutor(EurekaClient eurekaClient, TaskClient taskClient, int threadCount, int updateRetryCount,
        Map<String, String> taskToDomain, String workerNamePrefix) {
        this.eurekaClient = eurekaClient;
        this.taskClient = taskClient;
        this.updateRetryCount = updateRetryCount;
        this.taskToDomain = taskToDomain;

        LOGGER.info("Initialized the TaskPollExecutor with {} threads", threadCount);

        AtomicInteger count = new AtomicInteger(0);

        this.executorService = Executors.newFixedThreadPool(threadCount,
            new BasicThreadFactory.Builder()
                .namingPattern(workerNamePrefix + count.getAndIncrement())
                .uncaughtExceptionHandler(uncaughtExceptionHandler)
                .build());

        this.pollingSemaphore = new PollingSemaphore(threadCount);
    }

    void pollAndExecute(Worker worker) {
        if (eurekaClient != null && !eurekaClient.getInstanceRemoteStatus().equals(InstanceStatus.UP)) {
            LOGGER.debug("Instance is NOT UP in discovery - will not poll");
            return;
        }

        if (worker.paused()) {
            MetricsContainer.incrementTaskPausedCount(worker.getTaskDefName());
            LOGGER.debug("Worker {} has been paused. Not polling anymore!", worker.getClass());
            return;
        }

        Task task;
        try {
            if (!pollingSemaphore.canPoll()) {
                return;
            }

            String taskType = worker.getTaskDefName();
            String domain = Optional.ofNullable(PropertyFactory.getString(taskType, DOMAIN, null))
                .orElseGet(() -> Optional.ofNullable(PropertyFactory.getString(ALL_WORKERS, DOMAIN, null))
                    .orElse(taskToDomain.get(taskType)));

            LOGGER.debug("Polling task of type: {} in domain: '{}'", taskType, domain);
            task = MetricsContainer.getPollTimer(taskType)
                .record(() -> taskClient.pollTask(taskType, worker.getIdentity(), domain));

            if (Objects.nonNull(task) && StringUtils.isNotBlank(task.getTaskId())) {
                MetricsContainer.incrementTaskPollCount(taskType, 1);
                LOGGER.debug("Polled task: {} of type: {} in domain: '{}', from worker: {}",
                    task.getTaskId(), taskType, domain, worker.getIdentity());

                CompletableFuture<Task> taskCompletableFuture = CompletableFuture.supplyAsync(() ->
                    processTask(task, worker), executorService);

                taskCompletableFuture.whenComplete(this::finalizeTask);
            } else {
                // no task was returned in the poll, release the permit
                pollingSemaphore.complete();
            }
        } catch (Exception e) {
            // release the permit if exception is thrown during polling, because the thread would not be busy
            pollingSemaphore.complete();
            MetricsContainer.incrementTaskPollErrorCount(worker.getTaskDefName(), e);
            LOGGER.error("Error when polling for tasks", e);
        }
    }

    void shutdown() {
        shutdownExecutorService(executorService);
    }

    void shutdownExecutorService(ExecutorService executorService) {
        int timeout = 10;
        try {
            if (executorService.awaitTermination(timeout, TimeUnit.SECONDS)) {
                LOGGER.debug("tasks completed, shutting down");
            } else {
                LOGGER.warn(String.format("forcing shutdown after waiting for %s second", timeout));
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            LOGGER.warn("shutdown interrupted, invoking shutdownNow");
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("FieldCanBeLocal")
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (thread, error) -> {
        // JVM may be in unstable state, try to send metrics then exit
        MetricsContainer.incrementUncaughtExceptionCount();
        LOGGER.error("Uncaught exception. Thread {} will exit now", thread, error);
    };


    private Task processTask(Task task, Worker worker) {
        LOGGER.debug("Executing task: {} of type: {} in worker: {} at {}", task.getTaskId(), task.getTaskDefName(),
            worker.getClass().getSimpleName(), worker.getIdentity());
        try {
            executeTask(worker, task);
        } catch (Throwable t) {
            task.setStatus(Task.Status.FAILED);
            TaskResult result = new TaskResult(task);
            handleException(t, result, worker, task);
        } finally {
            pollingSemaphore.complete();
        }
        return task;
    }

    private void executeTask(Worker worker, Task task) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        TaskResult result = null;
        try {
            LOGGER.debug("Executing task: {} in worker: {} at {}", task.getTaskId(), worker.getClass().getSimpleName(),
                worker.getIdentity());
            result = worker.execute(task);
            result.setWorkflowInstanceId(task.getWorkflowInstanceId());
            result.setTaskId(task.getTaskId());
            result.setWorkerId(worker.getIdentity());
        } catch (Exception e) {
            LOGGER.error("Unable to execute task: {} of type: {}", task.getTaskId(), task.getTaskDefName(), e);
            if (result == null) {
                task.setStatus(Task.Status.FAILED);
                result = new TaskResult(task);
            }
            handleException(e, result, worker, task);
        } finally {
            stopwatch.stop();
            MetricsContainer.getExecutionTimer(worker.getTaskDefName())
                .record(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }

        LOGGER.debug("Task: {} executed by worker: {} at {} with status: {}", task.getTaskId(),
            worker.getClass().getSimpleName(), worker.getIdentity(), result.getStatus());
        updateWithRetry(updateRetryCount, task, result, worker);
    }

    private void finalizeTask(Task task, Throwable throwable) {
        if (throwable != null) {
            LOGGER.error("Error processing task: {} of type: {}", task.getTaskId(), task.getTaskType(), throwable);
            MetricsContainer.incrementTaskExecutionErrorCount(task.getTaskType(), throwable);
        } else {
            LOGGER.debug("Task:{} of type:{} finished processing with status:{}", task.getTaskId(),
                task.getTaskDefName(), task.getStatus());
        }
    }

    private void updateWithRetry(int count, Task task, TaskResult result, Worker worker) {
        try {
            String updateTaskDesc = String
                .format("Retry updating task result: %s for task: %s in worker: %s", result.toString(),
                    task.getTaskDefName(), worker.getIdentity());
            String evaluatePayloadDesc = String
                .format("Evaluate Task payload for task: %s in worker: %s", task.getTaskDefName(),
                    worker.getIdentity());
            String methodName = "updateWithRetry";

            TaskResult finalResult = new RetryUtil<TaskResult>().retryOnException(() ->
            {
                TaskResult taskResult = result.copy();
                taskClient.evaluateAndUploadLargePayload(taskResult, task.getTaskType());
                return taskResult;
            }, null, null, count, evaluatePayloadDesc, methodName);

            new RetryUtil<>().retryOnException(() ->
            {
                taskClient.updateTask(finalResult);
                return null;
            }, null, null, count, updateTaskDesc, methodName);
        } catch (Exception e) {
            worker.onErrorUpdate(task);
            MetricsContainer.incrementTaskUpdateErrorCount(worker.getTaskDefName(), e);
            LOGGER.error(String.format("Failed to update result: %s for task: %s in worker: %s", result.toString(),
                task.getTaskDefName(), worker.getIdentity()), e);
        }
    }

    private void handleException(Throwable t, TaskResult result, Worker worker, Task task) {
        LOGGER.error(String.format("Error while executing task %s", task.toString()), t);
        MetricsContainer.incrementTaskExecutionErrorCount(worker.getTaskDefName(), t);
        result.setStatus(TaskResult.Status.FAILED);
        result.setReasonForIncompletion("Error while executing the task: " + t);

        StringWriter stringWriter = new StringWriter();
        t.printStackTrace(new PrintWriter(stringWriter));
        result.log(stringWriter.toString());

        updateWithRetry(updateRetryCount, task, result, worker);
    }
}

/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.core.execution.batch;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.correlation.Correlator;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.TaskStatusListener;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.MetricService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Oleksiy Lysak
 */
@Singleton
public class BatchSweeper {
    private static final Logger logger = LoggerFactory.getLogger(BatchSweeper.class);
    private static final String JOB_ID_URN_PREFIX = "urn:deluxe:one-orders:deliveryjob:";
    private final Map<String, AbstractBatchProcessor> processors = new HashMap<>();
    private final TaskStatusListener taskStatusListener;
    private final WorkflowExecutor workflowExecutor;
    private final ExecutionDAO execDao;
    private final Configuration config;
    private final QueueDAO queues;

    @Inject
    public BatchSweeper(WorkflowExecutor workflowExecutor, Configuration config, QueueDAO queues, ExecutionDAO execDao,
                        SherlockBatchProcessor sherlockBatchProcessor, TaskStatusListener taskStatusListener) {
        this.taskStatusListener = taskStatusListener;
        this.workflowExecutor = workflowExecutor;
        this.config = config;
        this.queues = queues;
        this.execDao = execDao;

        processors.put("sherlock", sherlockBatchProcessor);

        String[] batchNames = config.getProperty("workflow.sweeper.batch.names", ",").split(",");
        if (ArrayUtils.isNotEmpty(batchNames)) {
            int batchInitDelay = config.getIntProperty("workflow.sweeper.batch.init.delay", 1000);
            int batchFrequency = config.getIntProperty("workflow.sweeper.batch.frequency", 5000);

            ScheduledExecutorService batchPool = Executors.newScheduledThreadPool(batchNames.length);
            for (String name : batchNames) {
                if (!processors.containsKey(name)) {
                    logger.error("Batch type " + name + " is not supported!");
                    continue;
                }
                batchPool.scheduleWithFixedDelay(() -> handle(name), batchInitDelay, batchFrequency, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void handle(String name) {
        AbstractBatchProcessor processor = processors.get(name);

        // How many tasks to query form the queue
        int count = config.getIntProperty("workflow.batch." + name + ".count", 100);

        // How much time we may spend to grab the above count of tasks
        int timeout = config.getIntProperty("workflow.batch." + name + ".timeout", 1000);

        // Rate limited
        int rateLimit = config.getIntProperty("workflow.batch." + name + ".rate.limit", 30_000);

        // Requeue task back if the task was rate limited
        int unackTimeout = config.getIntProperty("workflow.batch." + name + ".unack.timeout", 30_000);

        NDC.push("batch-" + name + "-" + UUID.randomUUID().toString());
        try {
            String workerId = InetAddress.getLocalHost().getHostName();
            String queueName = QueueUtils.getQueueName("batch." + name, null);

            List<TaskGroup> groups = poll(queueName, workerId, count, timeout, unackTimeout, rateLimit);
            if (groups.isEmpty()) {
                return;
            }

            // call processor
            processor.run(groups);

            // Cleanup batch queue + update last start time attribute
            long lastStartTime = System.currentTimeMillis();
            groups.stream().flatMap(g -> g.getTasks().stream()).forEach(task -> {
                String jobId = getJobId(task);
                String attribute = task.getReferenceTaskName() + "-jobId-" + jobId;
                Workflow workflow = workflowExecutor.getWorkflow(task.getWorkflowInstanceId(), false);
                workflow.getAttributes().put(attribute, lastStartTime);
                execDao.updateWorkflow(workflow);
                queues.remove(queueName, task.getTaskId());
            });
        } catch (Exception ex) {
            logger.error("Batch {} pool failed with {}", name, ex.getMessage(), ex);
        } finally {
            NDC.remove();
        }
    }

    private List<TaskGroup> poll(String queueName, String workerId, int count, int timeout, int unackTimeout, int rateLimit) {
        List<String> taskIds = queues.pop(queueName, count, timeout);
        if (CollectionUtils.isNotEmpty(taskIds)) {
            MetricService.getInstance().taskPoll(queueName, workerId, taskIds.size());
        }

        Map<String, List<Task>> groups = taskIds.parallelStream()
            .map(taskId -> {
                Task task = workflowExecutor.getTask(taskId);
                if (task == null) {
                    queues.remove(queueName, taskId); // We should remove the entry if no task found
                    return null;
                }

                Workflow wf = workflowExecutor.getWorkflow(task.getWorkflowInstanceId(), false);
                if (wf.getStatus().isTerminal()) {
                    String msg = "Workflow " + wf.getWorkflowId() + " is already completed as " + wf.getStatus() +
                        ", task=" + task.getTaskType() + ", reason=" + wf.getReasonForIncompletion() +
                        ", correlationId=" + wf.getCorrelationId();
                    logger.warn(msg);

                    queues.remove(queueName, taskId); // We should remove the entry if wf already finished
                    return null;
                }

                return task;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(t -> t.getInputData().get("uniqueness").toString()));

        List<TaskGroup> result = new ArrayList<>(groups.size());
        groups.forEach((key, tasks) -> {

            // Apply rate limit to the first task only as it only be used to call service
            Task carried = tasks.get(0);
            String jobId = getJobId(carried);
            String attribute = carried.getReferenceTaskName() + "-jobId-" + jobId;
            Workflow workflow = workflowExecutor.getWorkflow(carried.getWorkflowInstanceId(), false);
            Long prevStartTime = (Long)workflow.getAttributes().get(attribute);
            if (prevStartTime != null) {
                long timeSincePrevStart = System.currentTimeMillis() - prevStartTime;

                // Exit task right away if not allowed to start
                if (timeSincePrevStart < rateLimit) {
                    tasks.forEach(task -> {
                        Workflow wf = workflowExecutor.getWorkflow(task.getWorkflowInstanceId(), false);
                        logger.debug("batch task not allowed.workflowId=" + wf.getWorkflowId()
                            + ",correlationId=" + wf.getCorrelationId() + ",traceId=" + wf.getTraceId()
                            + ",uniquenessGroup=" + key + ",taskId=" + task.getTaskId()
                            + ",taskreference name=" + task.getReferenceTaskName()
                            + ",contextUser=" + wf.getContextUser());

                        // Task execution limited
                        queues.setUnackTimeout(queueName, task.getTaskId(), unackTimeout);
                    });
                    return;
                }
            }
            // Allow executing the entire group
            tasks.forEach(task -> {
                task.setStatus(Task.Status.IN_PROGRESS);
                if (task.getStartTime() == 0) {
                    task.setStartTime(System.currentTimeMillis());
                }

                // Metrics
                MetricService.getInstance().taskWait(task.getTaskType(),
                    task.getReferenceTaskName(),
                    task.getQueueWaitTime());

                task.setWorkerId(workerId);
                task.setPollCount(task.getPollCount() + 1);
                workflowExecutor.updateTask(task);
                taskStatusListener.onTaskStarted(task);

                if (!ackTaskReceived(queueName, task.getTaskId(), task.getResponseTimeoutSeconds())) {
                    logger.debug("Ack failed for {}, id {}", queueName, task.getTaskId());
                }
            });

            TaskGroup group = new TaskGroup();
            group.setKey(key);
            group.setTasks(tasks);

            result.add(group);
        });

        return result;
    }

    private boolean ackTaskReceived(String queueName, String taskId, int responseTimeoutSeconds) {
        if (responseTimeoutSeconds > 0) {
            logger.debug("Adding task " + queueName + "/" + taskId + " to be requeued if no response received " + responseTimeoutSeconds);
            return queues.setUnackTimeout(queueName, taskId, 1000L * responseTimeoutSeconds); //Value is in millisecond
        } else {
            return queues.ack(queueName, taskId);
        }
    }

    @SuppressWarnings("unchecked")
    private String getJobId(Task task) {
        Map<String, Object> body = (Map<String, Object>)task.getInputData().get("body");
        if (MapUtils.isEmpty(body))
            throw new IllegalArgumentException("No body provided in input parameters for " + task);

        // Trying to fetch job id from the request body (SH 1.20)
        String jobId = (String)body.get("jobId");
        if (StringUtils.isNotEmpty(jobId))
            return jobId;

        // Otherwise (legacy) retrieve it from the correlation id
        if (StringUtils.isNotEmpty(task.getCorrelationId()))
            return getJobId(task.getCorrelationId());

        return null;
    }

    private String getJobId(String correlationId) {
        Correlator correlator = new Correlator(logger, correlationId);
        String jobIdUrn = correlator.getContext().getUrn(JOB_ID_URN_PREFIX);
        if (StringUtils.isNotEmpty(jobIdUrn))
            return jobIdUrn.substring(JOB_ID_URN_PREFIX.length());
        return null;
    }
}
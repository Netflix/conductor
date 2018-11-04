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
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionService;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Oleksiy Lysak
 */
@Singleton
public class BatchSweeper {
    private static Logger logger = LoggerFactory.getLogger(BatchSweeper.class);

    private Map<String, AbstractBatchProcessor> processors = new HashMap<>();
    private ExecutionService executionService;
    private Configuration config;
    private QueueDAO queues;

    @Inject
    public BatchSweeper(ExecutionService executionService, Configuration config, QueueDAO queues,
                        SherlockBatchProcessor sherlockBatchProcessor) {
        this.executionService = executionService;
        this.config = config;
        this.queues = queues;

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
        int count = config.getIntProperty("workflow.sweeper.batch." + name + ".count", 100);

        // How much time we may spend to grab the above count of tasks
        int timeout = config.getIntProperty("workflow.sweeper.batch." + name + ".timeout", 1000);

        try {
            String taskType = "batch." + name;
            String workerId = InetAddress.getLocalHost().getHostName();
            List<Task> tasks = executionService.poll(taskType, workerId, count, timeout);
            if (tasks.isEmpty()) {
                return;
            }
            String queueName = QueueUtils.getQueueName(taskType, null);

            // Check workflow status and ack received task
            tasks.forEach(task -> {
                Workflow wf = executionService.getExecutionStatus(task.getWorkflowInstanceId(), false);
                if (wf.getStatus().isTerminal()) {
                    String msg = "Workflow " + wf.getWorkflowId() + " is already completed as " + wf.getStatus() +
                            ", task=" + task.getTaskType() + ", reason=" + wf.getReasonForIncompletion() +
                            ", correlationId=" + wf.getCorrelationId();
                    logger.warn(msg);
                    Monitors.recordUpdateConflict(task.getTaskType(), wf.getWorkflowType(), wf.getStatus());
                    return;
                }

                try {
                    if (!ackTaskReceived(queueName, task.getTaskId(), task.getResponseTimeoutSeconds())) {
                        logger.error("Ack failed for {}, id {}", taskType, task.getTaskId());
                    }
                } catch (Exception e) {
                    logger.error("Ack failed for {}, id {} with {}", taskType, task.getTaskId(), e.getMessage(), e);
                }
            });

            // call processor
            processor.run(tasks);

            // cleanup batch queue
            tasks.forEach(task -> queues.remove(queueName, task.getTaskId()));

        } catch (Exception ex) {
            logger.error("Batch {} pool failed with {}", name, ex.getMessage(), ex);
        }
    }

    private boolean ackTaskReceived(String queueName, String taskId, int responseTimeoutSeconds) {
        if (responseTimeoutSeconds > 0) {
            logger.debug("Adding task " + queueName + "/" + taskId + " to be requeued if no response received " + responseTimeoutSeconds);
            return queues.setUnackTimeout(queueName, taskId, 1000 * responseTimeoutSeconds); //Value is in millisecond
        } else {
            return queues.ack(queueName, taskId);
        }
    }
}
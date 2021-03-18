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
package com.netflix.conductor.core.execution;

import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.QueueDAO;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author Viren
 * @author Vikram
 *
 */
@Singleton
public class WorkflowSweeper {

    private static Logger logger = LoggerFactory.getLogger(WorkflowSweeper.class);

    private ExecutorService es;

    private Configuration config;

    private QueueDAO queues;

    private int executorThreadPoolSize;
    private long sweeperFrequency;
    private int poolTimeout;

    private static final String className = WorkflowSweeper.class.getSimpleName();

    @Inject
    public WorkflowSweeper(WorkflowExecutor executor, Configuration config, QueueDAO queues) {
        this.config = config;
        this.queues = queues;
        this.executorThreadPoolSize = config.getIntProperty("workflow.sweeper.thread.count", 5);
        this.sweeperFrequency = config.getIntProperty("workflow.sweeper.frequency", 500);
        this.poolTimeout = config.getIntProperty("workflow.sweeper.pool.timeout", 2000);
        if (this.executorThreadPoolSize > 0) {
            this.es = Executors.newFixedThreadPool(executorThreadPoolSize);
            init(executor);
            logger.debug("Workflow Sweeper Initialized");
        } else {
            logger.warn("Workflow sweeper is DISABLED");
        }

    }

    public void init(WorkflowExecutor executor) {
        boolean disable = config.disableSweep();
        if (disable) {
            logger.debug("Workflow sweep is disabled.");
            return;
        }

        ScheduledExecutorService deciderPool = Executors.newScheduledThreadPool(1);
        deciderPool.scheduleWithFixedDelay(() -> {
            try {
                List<String> workflowIds = queues.pop(WorkflowExecutor.deciderQueue, 2 * executorThreadPoolSize, poolTimeout);
                sweep(workflowIds, executor);
            } catch (Throwable e) {
                logger.debug("Workflow sweep failed " + e.getMessage(), e);
            }
        }, 500, sweeperFrequency, TimeUnit.MILLISECONDS);
    }

    public void sweep(List<String> workflowIds, WorkflowExecutor executor) throws Exception {

        List<Future<?>> futures = new LinkedList<>();
        for (String workflowId : workflowIds) {
            Future<?> future = es.submit(() -> {

                NDC.push("sweep-" + UUID.randomUUID().toString());
                try {

                    WorkflowContext ctx = new WorkflowContext(config.getAppId());
                    WorkflowContext.set(ctx);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Running sweeper for workflow {}", workflowId);
                    }
                    queues.push(WorkflowExecutor.sweeperQueue, workflowId, 0, 0); // For sweeper queue 0 priority is fine
                    Pair<Boolean, Integer> result = executor.decide(workflowId);
                    if (!result.getLeft()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Setting unack timeout {} secs for workflow {}", result.getRight(), workflowId);
                        }
                        queues.setUnackTimeout(WorkflowExecutor.deciderQueue, workflowId, result.getRight() * 1000);
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Marking workflow as completed {}", workflowId);
                        }
                        queues.remove(WorkflowExecutor.deciderQueue, workflowId);
                    }

                } catch (ApplicationException e) {
                    if (e.getCode().equals(Code.NOT_FOUND)) {
                        logger.debug("Workflow NOT found for id: " + workflowId, e);
                        queues.remove(WorkflowExecutor.deciderQueue, workflowId);
                    }
                } catch (Exception e) {
                    logger.debug("Error running sweep for " + workflowId, e);
                } finally {
                    queues.remove(WorkflowExecutor.sweeperQueue, workflowId);
                    NDC.remove();
                }
            });
            futures.add(future);
        }

        for (Future<?> future : futures) {
            future.get();
        }

    }

}
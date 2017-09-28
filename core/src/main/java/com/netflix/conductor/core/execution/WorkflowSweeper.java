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
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;

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

    private static final String className = WorkflowSweeper.class.getSimpleName();

    private static final String MODE_DIRECT = "direct";
    private static final String MODE_UNACK = "unack";

    @Inject
    public WorkflowSweeper(WorkflowExecutor executor, MetadataDAO metadata, Configuration config, QueueDAO queues) {
        this.config = config;
        this.queues = queues;
        this.executorThreadPoolSize = config.getIntProperty("workflow.sweeper.thread.count", 5);
        int delaySeconds = config.getIntProperty("workflow.sweeper.delay.seconds", 1);
        String mode = config.getProperty("workflow.sweeper.mode", MODE_UNACK);
        if (this.executorThreadPoolSize > 0) {
            this.es = Executors.newFixedThreadPool(executorThreadPoolSize);
            init(executor, metadata, mode, delaySeconds);
            logger.info("Workflow Sweeper Initialized");
        } else {
            logger.warn("Workflow sweeper is DISABLED");
        }
    }

    public void init(WorkflowExecutor executor, MetadataDAO metadata, String mode, int delaySeconds) {
        logger.info("Workflow sweep mode is " + mode);

        ScheduledExecutorService deciderPool = Executors.newScheduledThreadPool(1);
        deciderPool.scheduleWithFixedDelay(() -> {

            try {
                boolean disable = config.disableSweep();
                if (disable) {
                    logger.info("Workflow sweep is disabled.");
                    return;
                }

                if (MODE_DIRECT.equalsIgnoreCase(mode)) {
                    List<String> workflowIds = metadata.getAll().stream().map(workflowDef -> {
                        try {
                            return executor.getRunningWorkflowIds(workflowDef.getName());
                        } catch (Exception e) {
                            logger.error("Unable to get running workflow ids for "
                                    + workflowDef.getName() + ": " + e.getMessage(), e);
                            return null;
                        }
                    }).filter(Objects::nonNull).flatMap(Collection::stream).collect(toList());
                    sweep(workflowIds, executor, false);
                } else {
                    List<String> workflowIds = queues.pop(WorkflowExecutor.deciderQueue, 2 * executorThreadPoolSize, 2000);
                    sweep(workflowIds, executor, true);
                }

            } catch (Exception e) {
                Monitors.error(className, "sweep");
                logger.error(e.getMessage(), e);
            }

        }, 500, delaySeconds * 1000, TimeUnit.MILLISECONDS);
    }

    public void sweep(List<String> workflowIds, WorkflowExecutor executor, boolean isUnack) throws Exception {

        List<Future<?>> futures = new LinkedList<>();
        for (String workflowId : workflowIds) {
            Future<?> future = es.submit(() -> {
                try {

                    WorkflowContext ctx = new WorkflowContext(config.getAppId());
                    WorkflowContext.set(ctx);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Running sweeper for workflow {}", workflowId);
                    }
                    boolean done = executor.decide(workflowId);
                    if (!done) {
                        if (isUnack) {
                            queues.setUnackTimeout(WorkflowExecutor.deciderQueue, workflowId, config.getSweepFrequency() * 1000);
                        }
                    } else {
                        queues.remove(WorkflowExecutor.deciderQueue, workflowId);
                    }

                } catch (ApplicationException e) {
                    if (e.getCode().equals(Code.NOT_FOUND)) {
                        logger.error("Workflow NOT found for id: " + workflowId, e);
                        queues.remove(WorkflowExecutor.deciderQueue, workflowId);
                    }

                } catch (Exception e) {
                    Monitors.error(className, "sweep");
                    logger.error("Error running sweep for " + workflowId, e);
                }
            });
            futures.add(future);
        }

        for (Future<?> future : futures) {
            future.get();
        }

    }

}

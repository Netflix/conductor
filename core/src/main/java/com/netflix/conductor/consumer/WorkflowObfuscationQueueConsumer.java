package com.netflix.conductor.consumer;

import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowSweeper;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.exception.ObfuscationServiceException;
import com.netflix.conductor.service.ObfuscationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Pops messages from the obfuscationQueue and calls the ObfuscationService for each of them,
 * when the processing is complete the messages are removed. If a ObfuscationServiceException is
 * thrown, the message is also removed from queue.
 */
@Singleton
public class WorkflowObfuscationQueueConsumer {

    private static Logger LOGGER = LoggerFactory.getLogger(WorkflowSweeper.class);
    private ExecutorService executorService;
    private Configuration config;
    private QueueDAO queueDAO;
    private ObfuscationService obfuscationService;
    private int executorThreadPoolSize;
    private boolean obfuscationEnabled;
    private String workflowObfuscationQueue;

    @Inject
    public WorkflowObfuscationQueueConsumer(Configuration config, ObfuscationService obfuscationService, QueueDAO queueDAO) {
        this.config = config;
        this.obfuscationService = obfuscationService;
        this.queueDAO = queueDAO;
        this.obfuscationEnabled = config.getBooleanProperty("workflow.obfuscation.enabled", false);
        this.executorThreadPoolSize = config.getIntProperty("workflow.obfuscation.consumer.thread.count", 5);
        this.workflowObfuscationQueue = config.getProperty("workflow.obfuscation.queue.name", "_obfuscationQueue");

        if(obfuscationEnabled) {
            this.executorService = Executors.newFixedThreadPool(executorThreadPoolSize);
            init();
            LOGGER.info("workflow obfuscation consumer started");
        } else {
            LOGGER.info("workflow obfuscation consumer disabled, workflow obfuscation will not work");
        }
    }

    private void init() {
        ScheduledExecutorService coordinatorPool = Executors.newScheduledThreadPool(1);
        coordinatorPool.scheduleWithFixedDelay(() -> {
            List<String> workflowIds = queueDAO.pop(workflowObfuscationQueue, 2 * executorThreadPoolSize, 2000);
            LOGGER.debug("{} workflow obfuscation requests popped", workflowIds.size());
            process(workflowIds);
        }, 500, 500, TimeUnit.MILLISECONDS);
    }


    private void process(List<String> workflowIds) {
        List<Future<?>> futures = new LinkedList<>();
        workflowIds.forEach(id -> {
            Future<?> future = executorService.submit(() -> {
                try {
                    LOGGER.debug("processing obfuscation for workflowId: {}", id);
                    obfuscationService.obfuscateFields(id);
                    queueDAO.remove(workflowObfuscationQueue, id);
                } catch (Exception e) {
                    if (e instanceof ObfuscationServiceException) {
                        queueDAO.remove(workflowObfuscationQueue, id);
                    }
                }
            });
            futures.add(future);
        });
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}

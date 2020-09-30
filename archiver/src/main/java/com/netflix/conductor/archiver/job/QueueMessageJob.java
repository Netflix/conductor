package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class QueueMessageJob extends AbstractJob {
    private static final Logger logger = LogManager.getLogger(QueueMessageJob.class);

    public QueueMessageJob(HikariDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public void cleanup() {
        logger.info("Starting QueueMessageJob");
        try {
            AppConfig config = AppConfig.getInstance();
            String cleanupWorkflows = config.cleanupMessageWorkflows();
            String QUERY = "select id from queue_message qm " +
                    "where qm.queue_name in ('_deciderqueue', '_sweeperqueue') " +
                    "and qm.message_id in (select wf.workflow_id from workflow wf " +
                    "where wf.workflow_type = ANY (?)) LIMIT ?";
            int batchSize = config.batchSize();
            logger.info("Deleting records with batch size = " + batchSize);

            int deleted = 0;
            List<String> workflowList = new ArrayList<>();
            if (!cleanupWorkflows.isEmpty()) {
                workflowList = Arrays.asList(cleanupWorkflows.split(","));
            } else {
                workflowList.add("deluxe.dependencygraph.sourcewait.process.1.0");
                workflowList.add("deluxe.dependencygraph.sourcewait.process.1.1");
                workflowList.add("deluxe.dependencygraph.source_wait.sherlock.1.0");
                workflowList.add("deluxe.dependencygraph.source_wait.sherlock.1.1");
            }

            List<Integer> ids = fetchIds(QUERY, workflowList, batchSize);
            while (isNotEmpty(ids)) {
                deleted += deleteByIds("queue_message", ids);
                logger.debug("QueueMessageJob deleted " + deleted);

                ids = fetchIds(QUERY, workflowList, batchSize);
            }
            logger.info("Finished QueueMessageJob");
        } catch (Exception ex) {
            logger.error("QueueMessageJob failed " + ex.getMessage(), ex);
        }
    }
}

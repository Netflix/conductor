package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class QueueMessageJob extends AbstractJob {
    private static final Logger logger = LogManager.getLogger(DbLogJob.class);
    private static final String QUERY = "select id from queue_message qm\n" +
            "where qm.queue_name in ('_deciderqueue', '_sweeperqueue')\n" +
            "and qm.message_id in (select wf.workflow_id from workflow wf\n" +
            "where wf.workflow_type in ('deluxe.dependencygraph.sourcewait.process.1.0', 'deluxe.dependencygraph.sourcewait.process.1.1',\n" +
            "'deluxe.dependencygraph.source_wait.sherlock.1.0', 'deluxe.dependencygraph.source_wait.sherlock.1.1')) LIMIT ?";

    public DbLogJob(HikariDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public void cleanup() {
        logger.info("Starting QueueMessageJob");
        try {
            AppConfig config = AppConfig.getInstance();
            int batchSize = config.batchSize();
            Timestamp endTime = new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays()));
            logger.info("Deleting records earlier than " + endTime + ", batch size = " + batchSize);

            int deleted = 0;
            List<Integer> ids = fetchIds(QUERY, batchSize);
            while (isNotEmpty(ids)) {
                deleted += deleteByIds("queue_message", ids);
                logger.info("QueueMessageJob deleted " + deleted);

                ids = fetchIds(QUERY, endTime, batchSize);
            }
            logger.info("Finished QueueMessageJob");
        } catch (Exception ex) {
            logger.error("QueueMessageJob failed " + ex.getMessage(), ex);
        }
    }
}

package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class EventPubsJob extends AbstractJob {
	private static final Logger logger = LogManager.getLogger(EventPubsJob.class);
	private static final String QUERY = "SELECT id FROM event_published WHERE created_on < ? LIMIT ?";

	public EventPubsJob(HikariDataSource dataSource) {
		super(dataSource);
	}

	@Override
	public void cleanup() {
		logger.info("Starting event pubs job");
		try {
			AppConfig config = AppConfig.getInstance();
			int batchSize = config.batchSize();
			Timestamp endTime = new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays()));
			logger.info("Deleting records earlier than " + endTime + ", batch size = " + batchSize);

			int deleted = 0;
			List<Integer> ids = fetchIds(QUERY, endTime, batchSize);
			while (isNotEmpty(ids)) {
				deleted += deleteByIds("event_published", ids);
				logger.debug("EventPubs job deleted " + deleted);

				ids = fetchIds(QUERY, endTime, batchSize);
			}
			logger.info("Finished event pubs job");
		} catch (Exception ex) {
			logger.error("EventPubs job failed " + ex.getMessage(), ex);
		}
	}
}

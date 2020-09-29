package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class EventMesgsJob extends AbstractJob {
	private static final Logger logger = LogManager.getLogger(EventMesgsJob.class);
	private static final String QUERY = "SELECT id FROM event_message WHERE created_on < ? LIMIT ?";

	public EventMesgsJob(HikariDataSource dataSource) {
		super(dataSource);
	}

	@Override
	public void cleanup() {
		logger.debug("Starting event message job");
		try {
			AppConfig config = AppConfig.getInstance();
			int batchSize = config.batchSize();
			Timestamp endTime = new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays()));
			logger.debug("Deleting records earlier than " + endTime + ", batch size = " + batchSize);

			int deleted = 0;
			List<Integer> ids = fetchIds(QUERY, endTime, batchSize);
			while (isNotEmpty(ids)) {
				deleted += deleteByIds("event_message", ids);
				logger.debug("EventMesgs job deleted " + deleted);

				ids = fetchIds(QUERY, endTime, batchSize);
			}
			logger.debug("Finished event message job");
		} catch (Exception ex) {
			logger.error("EventMesgs job failed " + ex.getMessage(), ex);
		}
	}
}

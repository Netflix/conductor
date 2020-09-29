package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class DbLogJob extends AbstractJob {
	private static final Logger logger = LogManager.getLogger(DbLogJob.class);
	private static final String QUERY = "SELECT id FROM log4j_logs WHERE log_time < ? LIMIT ?";

	public DbLogJob(HikariDataSource dataSource) {
		super(dataSource);
	}

	@Override
	public void cleanup() {
		logger.debug("Starting db log job");
		try {
			AppConfig config = AppConfig.getInstance();
			int batchSize = config.batchSize();
			Timestamp endTime = new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays()));
			logger.debug("Deleting records earlier than " + endTime + ", batch size = " + batchSize);

			int deleted = 0;
			List<Integer> ids = fetchIds(QUERY, endTime, batchSize);
			while (isNotEmpty(ids)) {
				deleted += deleteByIds("log4j_logs", ids);
				logger.debug("Db log job deleted " + deleted);

				ids = fetchIds(QUERY, endTime, batchSize);
			}
			logger.debug("Finished db log job");
		} catch (Exception ex) {
			logger.error("DbLog job failed " + ex.getMessage(), ex);
		}
	}
}

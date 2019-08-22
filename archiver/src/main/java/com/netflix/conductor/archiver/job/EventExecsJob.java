package com.netflix.conductor.archiver.job;

import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class EventExecsJob extends AbstractJob {
	private static final Logger logger = LogManager.getLogger(EventExecsJob.class);
	private static final String CLEANUP = "DELETE FROM event_execution WHERE created_on < ?";

	public EventExecsJob(HikariDataSource dataSource) {
		super(dataSource);
	}

	@Override
	public void cleanup() {
		logger.info("Starting event execs job");
		try {
			AppConfig config = AppConfig.getInstance();
			try (Connection tx = dataSource.getConnection(); PreparedStatement st = tx.prepareStatement(CLEANUP)) {

				long endTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays());
				logger.info("Deleting records earlier than " + new Timestamp(endTime));

				st.setTimestamp(1, new Timestamp(endTime));

				int records = st.executeUpdate();
				logger.info("EventExecs job deleted " + records);
			}
		} catch (Exception ex) {
			logger.error("EventExecs job failed " + ex.getMessage(), ex);
		}
	}
}

package com.netflix.conductor.archiver.cleanup;

import com.netflix.conductor.archiver.config.AppConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class DbLogCleanup extends AbstractCleanup {
	private static final Logger logger = LogManager.getLogger(DbLogCleanup.class);
	private static final String CLEANUP = "delete from log4j_logs where log_time < ?";

	public DbLogCleanup(RestHighLevelClient client) {
		super(client);
	}

	@Override
	public void cleanup() {
		logger.info("Starting db log cleanup");
		try {
			AppConfig config = AppConfig.getInstance();
			if (!config.isLog4jAuroraAppender()) {
				logger.warn("Doing nothing as log4j_aurora_appender is not enabled");
				return;
			}

			if (StringUtils.isAnyEmpty(config.auroraHost(), config.auroraPort(), config.auroraDb(), config.auroraUser(), config.auroraPassword())) {
				logger.error("Not all aurora properties defined");
				return;
			}

			// jdbc:postgresql://"${aurora_host}":"${aurora_port}"/"${aurora_db}
			String url = String.format("jdbc:postgresql://%s:%s/%s", config.auroraHost(), config.auroraPort(), config.auroraDb());

			try (Connection tx = DriverManager.getConnection(url, config.auroraUser(), config.auroraPassword())) {
				tx.setAutoCommit(true);

				long endTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays());
				logger.info("Deleting records earlier than " + new Timestamp(endTime));

				PreparedStatement st = tx.prepareStatement(CLEANUP);
				st.setTimestamp(1, new Timestamp(endTime));

				int deleted = st.executeUpdate();
				logger.info("Db log cleanup deleted " + deleted);
			}
		} catch (Exception ex) {
			logger.error("Db log cleanup failed " + ex.getMessage(), ex);
		}
	}
}

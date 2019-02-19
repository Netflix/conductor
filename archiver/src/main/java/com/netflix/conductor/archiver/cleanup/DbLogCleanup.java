package com.netflix.conductor.archiver.cleanup;

import com.netflix.conductor.archiver.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class DbLogCleanup {
	private static final Logger logger = LogManager.getLogger(DbLogCleanup.class);
	private static final String CLEANUP = "delete from log4j_logs where log_time < ?";

	public void cleanup() {
		logger.info("Starting db log cleanup");
		try {
			AppConfig config = AppConfig.getInstance();

			// jdbc:postgresql://"${aurora_host}":"${aurora_port}"/"${aurora_db}
			String url = String.format("jdbc:postgresql://%s:%s/%s", config.auroraHost(), config.auroraPort(), config.auroraDb());

			try (Connection tx = DriverManager.getConnection(url, config.auroraUser(), config.auroraPassword())) {
				tx.setAutoCommit(true);
				long startTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.keepDays());
				PreparedStatement st = tx.prepareStatement(CLEANUP);
				st.setTimestamp(1, new Timestamp(startTime));
				int deleted = st.executeUpdate();
				logger.info("Db log cleanup deleted " + deleted);
			}
		} catch (Exception ex) {
			logger.error("Db log cleanup failed " + ex.getMessage(), ex);
		}
	}
}

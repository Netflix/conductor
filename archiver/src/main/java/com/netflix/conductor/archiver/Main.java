package com.netflix.conductor.archiver;

import com.netflix.conductor.archiver.job.*;
import com.netflix.conductor.archiver.config.AppConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Main {
	private static Logger logger;

	public static void main(String[] args) {
		PluginManager.addPackage(Main.class.getPackage().getName());
		logger = LogManager.getLogger(Main.class);
		logger.info("Starting archiver");
		try {
			Main main = new Main();
			main.start();
			System.exit(0);
		} catch (Throwable ex) {
			logger.error("Archiver failed with " + ex.getMessage(), ex);
			System.exit(-1);
		}
	}

	private void start() throws Exception {
		long start = System.currentTimeMillis();

		AppConfig config = AppConfig.getInstance();
		String url = String.format("jdbc:postgresql://%s:%s/%s",
			config.auroraHost(), config.auroraPort(), config.auroraDb());

		HikariConfig poolConfig = new HikariConfig();
		poolConfig.setJdbcUrl(url);
		poolConfig.setUsername(config.auroraUser());
		poolConfig.setPassword(config.auroraPassword());
		poolConfig.setAutoCommit(true);
		poolConfig.setPoolName("archiver");
		poolConfig.setConnectionTimeout(60_000);
		poolConfig.setMinimumIdle(config.queueWorkers());
		poolConfig.setMaximumPoolSize(config.queueWorkers() * 2);
		poolConfig.addDataSourceProperty("ApplicationName", "archiver-" + InetAddress.getLocalHost().getHostName());
		poolConfig.addDataSourceProperty("cachePrepStmts", "true");
		poolConfig.addDataSourceProperty("prepStmtCacheSize", "250");
		poolConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

		HikariDataSource dataSource = new HikariDataSource(poolConfig);

		List<AbstractJob> jobs = new ArrayList<>();
		jobs.add(new EventMesgsJob(dataSource));
		jobs.add(new EventExecsJob(dataSource));
		jobs.add(new EventPubsJob(dataSource));
		jobs.add(new WorkflowJob(dataSource));
		jobs.add(new DbLogJob(dataSource));
		jobs.add(new QueueMessageJob(dataSource));

		// Run jobs in threads
		CountDownLatch countDown = new CountDownLatch(jobs.size());
		jobs.forEach(job -> {
			new Thread(() -> {
				try {
					job.cleanup();
				} catch (Throwable e) {
					logger.error(job.getClass().getName() + " failed with " + e.getMessage(), e);
				} finally {
					countDown.countDown();
				}
			}).start();
		});

		// Wait for the exporters
		countDown.await();

		String FORMAT = "H'h' m'm' s's'";
		String duration = DurationFormatUtils.formatDuration(System.currentTimeMillis() - start, FORMAT, true);
		logger.info("Finishing archiver, took " + duration);
	}
}

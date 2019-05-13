package com.netflix.conductor.archiver;

import com.netflix.conductor.archiver.cleanup.*;
import com.netflix.conductor.archiver.config.AppConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Main {
	private static Logger logger;
	private static String FORMAT = "H'h' m'm' s's'";
	private RestHighLevelClient client;

	public static void main(String[] args) {
		PluginManager.addPackage(Main.class.getPackage().getName());
		logger = LogManager.getLogger(Main.class);
		logger.info("Starting archiver");
		try {
			Main main = new Main();
			main.start();
			System.exit(0);
		} catch (Throwable ex) {
			logger.error("main failed with " + ex.getMessage(), ex);
			System.exit(-1);
		}
	}

	private void checkConfig(String value, String config) {
		if (StringUtils.isEmpty(value))
			throw new RuntimeException("No '" + config + "' configuration provided");
	}

	private void start() throws Exception {
		checkConfig(AppConfig.getInstance().source(), "source");
		checkConfig(AppConfig.getInstance().env(), "env");

		long start = System.currentTimeMillis();
		String clusterAddress = AppConfig.getInstance().source();
		logger.info("Creating ElasticSearch client for " + clusterAddress);
		if (StringUtils.isEmpty(clusterAddress)) {
			throw new RuntimeException("No ElasticSearch Url defined. Exiting");
		}

		RestClientBuilder builder = RestClient.builder(HttpHost.create(clusterAddress));
		client = new RestHighLevelClient(builder);

		deleteOldData();
		deleteEmptyIndexes();

		String duration = DurationFormatUtils.formatDuration(System.currentTimeMillis() - start, FORMAT, true);
		logger.info("Finishing archiver, took " + duration);
	}

	private void deleteOldData() throws InterruptedException {
		List<AbstractCleanup> workers = new ArrayList<>();
		workers.add(new WorkflowCleanup(client));
		workers.add(new EventExecsCleanup(client));
		workers.add(new EventPubsCleanup(client));
		workers.add(new TaskLogCleanup(client));
		workers.add(new DbLogCleanup(client));

		// Run the workers in threads
		CountDownLatch countDown = new CountDownLatch(workers.size());
		workers.forEach(worker -> {
			new Thread(() -> {
				try {
					worker.cleanup();
				} catch (Throwable e) {
					logger.error(worker.getClass().getName() + " failed with " + e.getMessage(), e);
				} finally {
					countDown.countDown();
				}
			}).start();
		});

		// Wait for the exporters
		countDown.await();
	}

	private void deleteEmptyIndexes() throws IOException {
		new TaskLogCleanup(client).deleteEmptyIndexes();
	}
}

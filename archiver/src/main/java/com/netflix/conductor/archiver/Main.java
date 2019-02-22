package com.netflix.conductor.archiver;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.archiver.cleanup.DbLogCleanup;
import com.netflix.conductor.archiver.config.AppConfig;
import com.netflix.conductor.archiver.export.AbstractExport;
import com.netflix.conductor.archiver.export.EventExport;
import com.netflix.conductor.archiver.export.TaskLogExport;
import com.netflix.conductor.archiver.export.WorkflowExport;
import com.netflix.conductor.archiver.writers.EntityWriters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazonaws.SDKGlobalConfiguration.*;

public class Main {
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
	private static Logger logger;
	private RestHighLevelClient client;
	private String sessionId;

	public static void main(String[] args) {
		PluginManager.addPackage(Main.class.getPackage().getName());
		logger = LogManager.getLogger(Main.class);
		logger.info("Starting ...");
		try {
			Main main = new Main();
			main.start();
		} catch (Throwable ex) {
			logger.error("main failed with " + ex.getMessage(), ex);
		}
	}

	private void checkConfig(String value, String config) {
		if (StringUtils.isEmpty(value))
			throw new RuntimeException("No '" + config + "' configuration provided");
	}

	private void start() {
		checkConfig(AppConfig.getInstance().source(), "source");
		checkConfig(AppConfig.getInstance().env(), "env");
		checkConfig(AppConfig.getInstance().bucketName(), "bucket_name");
		checkConfig(AppConfig.getInstance().region(), "region");
		checkConfig(AppConfig.getInstance().accessKey(), "access_key");
		checkConfig(AppConfig.getInstance().accessSecret(), "access_secret");

		boolean startFrom = StringUtils.isNotEmpty(AppConfig.getInstance().sessionId());
		if (startFrom) {
			this.sessionId = AppConfig.getInstance().sessionId();
		} else {
			this.sessionId = sdf.format(new Date());
		}
		logger.info("Starting archiver with sessionId=" + sessionId);

		try {
			FileUtils.forceMkdir(new File(sessionId));

			String clusterAddress = AppConfig.getInstance().source();
			logger.info("Creating ElasticSearch client for " + clusterAddress + ". SessionId=" + sessionId);
			if (StringUtils.isEmpty(clusterAddress)) {
				throw new RuntimeException("No ElasticSearch Url defined. Exiting");
			}

			RestClientBuilder builder = RestClient.builder(HttpHost.create(clusterAddress));
			this.client = new RestHighLevelClient(builder);

			EntityWriters writers = new EntityWriters(sessionId, startFrom);

			if (!startFrom) {
				export(writers);
				zip(writers);
			}
			upload(writers);
			deleteOldData(writers);
			deleteEmptyIndexes(writers);
			deleteDbLogs();

			logger.info("Finishing archiver for sessionId=" + sessionId);
			System.exit(0);

		} catch (Throwable ex) {
			logger.error("Global exception " + ex.getMessage() + " for sessionId=" + sessionId, ex);
			System.exit(-1);
		}
	}

	private void export(EntityWriters writers) throws InterruptedException {
		List<AbstractExport> exporterList = new ArrayList<>();
		exporterList.add(new WorkflowExport(client, writers));
		exporterList.add(new EventExport(client, writers));
		exporterList.add(new TaskLogExport(client, writers));

		// Run the exporters in threads
		CountDownLatch countDown = new CountDownLatch(exporterList.size());
		exporterList.forEach(purger -> {
			new Thread(() -> {
				try {
					purger.export();
				} catch (Throwable e) {
					logger.error(purger.getClass().getName() + " failed with " + e.getMessage(), e);
				} finally {
					countDown.countDown();
				}
			}).start();
		});

		// Wait for the exporters
		countDown.await();

		// Close files
		writers.close();
	}

	private void zip(EntityWriters writers) {
		logger.info("Zipping files ...");
		writers.wrappers().forEach((key, value) -> {
			// We do not zip the workflows.json (manifest file)
			if (value.isManifest()) {
				return;
			}
			logger.info("Zipping " + value);
			try {
				value.zip();
			} catch (IOException e) {
				logger.error("Zip failed for " + key + " with " + e.getMessage(), e);
			}
		});
	}

	private void upload(EntityWriters writers) {
		logger.info("Uploading to AWS S3 " + AppConfig.getInstance().bucketName());
		System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, AppConfig.getInstance().accessKey());
		System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, AppConfig.getInstance().accessSecret());
		System.setProperty(AWS_REGION_SYSTEM_PROPERTY, AppConfig.getInstance().region());
		SystemPropertiesCredentialsProvider provider = new SystemPropertiesCredentialsProvider();
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(provider).build();
		writers.wrappers().values().forEach(value -> {
			logger.info("Uploading " + value);
			value.upload(s3, AppConfig.getInstance().bucketName(), sessionId);
		});
	}

	private void deleteOldData(EntityWriters writers) throws InterruptedException {
		// Read files and remove from elasticsearch
		int batchSize = AppConfig.getInstance().batchSize();
		ObjectMapper mapper = new ObjectMapper();
		CountDownLatch countDown = new CountDownLatch(writers.wrappers().values().size() - 1);
		writers.wrappers().values().forEach(wrapper -> {
			if (wrapper.isManifest()) {
				return;
			}

			new Thread(() -> {
				try {
					logger.info("Started deleting old db records based on " + wrapper + " file");
					AtomicLong totalProcessed = new AtomicLong(0);
					AtomicBoolean bulkProcessed = new AtomicBoolean(false);
					AtomicReference<BulkRequest> bulkRequest = new AtomicReference<>(new BulkRequest());
					wrapper.stream().forEach(line -> {
						try {
							HashMap map = mapper.readValue(line, HashMap.class);
							DeleteRequest deleteRequest = new DeleteRequest();
							deleteRequest.index(map.get("_index").toString());
							deleteRequest.type(map.get("_type").toString());
							deleteRequest.id(map.get("_id").toString());

							bulkRequest.get().add(deleteRequest);
							totalProcessed.incrementAndGet();
							bulkProcessed.set(false);

							if (bulkRequest.get().requests().size() == batchSize) {
								client.bulk(bulkRequest.get());

								bulkProcessed.set(true);
								bulkRequest.set(new BulkRequest());
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					});

					// Process the rest if any left outside batch size
					if (!bulkProcessed.get() && !bulkRequest.get().requests().isEmpty()) {
						client.bulk(bulkRequest.get());
					}

					logger.info(wrapper + " total deleted " + totalProcessed.get());
				} catch (Exception e) {
					logger.error(wrapper + " failed with " + e.getMessage(), e);
				} finally {
					countDown.countDown();
				}
			}).start();

		});

		// Wait for the bulks to delete old data
		countDown.await();
	}

	private void deleteEmptyIndexes(EntityWriters writers) throws IOException {
		new TaskLogExport(client, writers).deleteEmptyTaskLogsIndexes();
	}

	private void deleteDbLogs() throws IOException {
		DbLogCleanup action = new DbLogCleanup();
		action.cleanup();
	}
}

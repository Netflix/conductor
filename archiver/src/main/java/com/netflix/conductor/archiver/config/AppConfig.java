package com.netflix.conductor.archiver.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class AppConfig {
	private Configuration config;
	private static AppConfig INSTANCE;

	private AppConfig() {
		Parameters params = new Parameters();

		FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
			new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
				.configure(params.properties().setFileName("./archiver.properties"));
		try {
			config = builder.getConfiguration();
		} catch (ConfigurationException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static AppConfig getInstance() {
		if (INSTANCE == null)
			INSTANCE = new AppConfig();
		return INSTANCE;
	}

	public String source() {
		return config.getString("source");
	}

	public String bucketName() {
		return config.getString("bucket_name");
	}

	public String accessKey() {
		return config.getString("access_key");
	}

	public String region() {
		return config.getString("region");
	}

	public String sessionId() {
		return config.getString("sessionId");
	}

	public String accessSecret() {
		return config.getString("access_secret");
	}

	public String env() {
		return config.getString("env");
	}

	public int batchSize() {
		return config.getInt("batch_size", 500);
	}

	public int queueWorkers() {
		return config.getInt("queue_workers", 50);
	}

	public int keepDays() {
		return config.getInt("keep_days", 30);
	}

	public String rootIndexName() {
		return config.getString("workflow_elasticsearch_index_name", "conductor");
	}

	public String taskLogPrefix() {
		return config.getString("workflow_elasticsearch_tasklog_index_name", "task_log");
	}

}

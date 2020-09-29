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

	public int batchSize() {
		return config.getInt("batch_size", 100);
	}

	public int queueWorkers() {
		return config.getInt("queue_workers", 50);
	}

	public int keepDays() {
		return config.getInt("keep_days", 30);
	}

	public String cleanupMessageWorkflows() {
		return config.getString("cleanup_message_workflows", "'deluxe.dependencygraph.sourcewait.process.1.0', 'deluxe.dependencygraph.sourcewait.process.1.1','deluxe.dependencygraph.source_wait.sherlock.1.0', 'deluxe.dependencygraph.source_wait.sherlock.1.1'");
	}

	public String auroraHost() {
		return config.getString("aurora_host");
	}

	public String auroraPort() {
		return config.getString("aurora_port");
	}

	public String auroraDb() {
		return config.getString("aurora_db");
	}

	public String auroraUser() {
		return config.getString("aurora_user");
	}

	public String auroraPassword() {
		return config.getString("aurora_password");
	}
}
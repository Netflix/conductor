package com.bydeluxe.es2pg.migration;

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
				.configure(params.properties().setFileName("./migration.properties"));
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

	public String env() {
		return config.getString("env");
	}

	public int batchSize() {
		return config.getInt("batch_size", 500);
	}

	public int queueWorkers() {
		return config.getInt("queue_workers", 50);
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

	public String rootIndexName() {
		return config.getString("workflow_elasticsearch_index_name", "conductor");
	}
}
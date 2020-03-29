package com.netflix.conductor.mysql;

import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.sql.DataSource;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLDataSourceProvider implements Provider<DataSource> {
    private static final Logger logger = LoggerFactory.getLogger(MySQLDataSourceProvider.class);

    private final MySQLConfiguration configuration;

    @Inject
    public MySQLDataSourceProvider(MySQLConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public DataSource get() {
		HikariDataSource dataSource;
		try {
			dataSource = new HikariDataSource(createConfiguration());
		}
		catch (final Throwable t) {
			logger.error("Could not initialize Conductor datasource", t);
			throw t;
		}

		try {
			flywayMigrate(dataSource);
		}
		catch (final Throwable t) {
			logger.error("Errors occurred migrating Conductor schema", t);
			if (!dataSource.isClosed()) {
				dataSource.close();
			}
		}

		return dataSource;
	}

    private HikariConfig createConfiguration(){
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(configuration.getJdbcUrl());
        cfg.setUsername(configuration.getJdbcUserName());
        cfg.setPassword(configuration.getJdbcPassword());
        cfg.setMaximumPoolSize(configuration.getConnectionPoolMaxSize());
        cfg.setMinimumIdle(configuration.getConnectionPoolMinIdle());
        cfg.setMaxLifetime(configuration.getConnectionMaxLifetime());
        cfg.setIdleTimeout(configuration.getConnectionIdleTimeout());
        cfg.setConnectionTimeout(configuration.getConnectionTimeout());
        cfg.setTransactionIsolation(configuration.getTransactionIsolationLevel());
        cfg.setAutoCommit(configuration.isAutoCommit());

        ThreadFactory tf = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("hikari-mysql-%d")
                .build();

        cfg.setThreadFactory(tf);
        return cfg;
    }

    private void flywayMigrate(DataSource dataSource) {
		boolean enabled = configuration.isFlywayEnabled();
		if (!enabled) {
			logger.debug("Flyway migrations are disabled");
			return;
		}


		Flyway flyway = new Flyway();
		configuration.getFlywayTable().ifPresent(tableName -> {
			logger.debug("Using Flyway migration table '{}'", tableName);
			flyway.setTable(tableName);
		});

		flyway.setDataSource(dataSource);
		flyway.setPlaceholderReplacement(false);
		flyway.setBaselineOnMigrate(configuration.isFlywayBaselineOnMigrate());

		if (configuration.isFlywayBaselineOnMigrate()) {
			logger.trace("Flyway baseline migration is enabled");
			flyway.setBaselineVersionAsString("0");
			flyway.setBaselineDescription("Conductor baseline migration");
		}

		String[] locations = configuration.getFlywayLocations();
		if (locations.length > 0) {
			flyway.setLocations(locations);
		}

		flyway.migrate();
	}
}

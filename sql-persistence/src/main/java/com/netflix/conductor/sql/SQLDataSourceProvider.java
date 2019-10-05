package com.netflix.conductor.sql;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.sql.DataSource;
import java.util.concurrent.ThreadFactory;

public abstract class SQLDataSourceProvider implements Provider<DataSource> {
    private static final Logger logger = LoggerFactory.getLogger(SQLDataSourceProvider.class);

    private final SQLConfiguration configuration;

    protected abstract String getMigrationPath();

    @Inject
    public SQLDataSourceProvider(SQLConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public DataSource get() {
        HikariDataSource dataSource = null;
        try {
            dataSource = new HikariDataSource(createConfiguration());
            flywayMigrate(dataSource);
            return dataSource;
        } catch (final Throwable t) {
            if(null != dataSource && !dataSource.isClosed()){
                dataSource.close();
            }
            logger.error("error migration DB", t);
            throw t;
        }
    }

    private HikariConfig createConfiguration(){
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(configuration.getJdbcUrl());
        cfg.setUsername(configuration.getJdbcUserName());
        cfg.setPassword(configuration.getJdbcPassword());
        cfg.setAutoCommit(false);
        cfg.setMaximumPoolSize(configuration.getConnectionPoolMaxSize());
        cfg.setMinimumIdle(configuration.getConnectionPoolMinIdle());
        cfg.setMaxLifetime(configuration.getConnectionMaxLifetime());
        cfg.setIdleTimeout(configuration.getConnectionIdleTimeout());
        cfg.setConnectionTimeout(configuration.getConnectionTimeout());
        cfg.setTransactionIsolation(configuration.getTransactionIsolationLevel());
        cfg.setAutoCommit(configuration.isAutoCommit());

        ThreadFactory tf = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("hikari-sql-%d")
                .build();

        cfg.setThreadFactory(tf);
        return cfg;
    }
    // TODO Move this into a class that has complete lifecycle for the connection, i.e. startup and shutdown.
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

        String migrationPath = getMigrationPath();
        flyway.setLocations(migrationPath);
        flyway.setDataSource(dataSource);
        flyway.setPlaceholderReplacement(false);
        flyway.migrate();
    }
}

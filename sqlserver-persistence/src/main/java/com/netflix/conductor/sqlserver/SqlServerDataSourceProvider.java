package com.netflix.conductor.sqlserver;

import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.sql.DataSource;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerDataSourceProvider implements Provider<DataSource> {
    private static final Logger logger = LoggerFactory.getLogger(SqlServerDataSourceProvider.class);

    private final SqlServerConfiguration configuration;

    @Inject
    public SqlServerDataSourceProvider(SqlServerConfiguration configuration) {
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
        cfg.setAutoCommit(configuration.isAutoCommit());
        cfg.setMaximumPoolSize(configuration.getConnectionPoolMaxSize());
        cfg.setMinimumIdle(configuration.getConnectionPoolMinIdle());
        cfg.setMaxLifetime(configuration.getConnectionMaxLifetime());
        cfg.setIdleTimeout(configuration.getConnectionIdleTimeout());
        cfg.setConnectionTimeout(configuration.getConnectionTimeout());
        cfg.setTransactionIsolation(configuration.getTransactionIsolationLevel());
        cfg.setSchema("data");
        cfg.setConnectionInitSql(String.format(
            "SET IMPLICIT_TRANSACTIONS OFF; SET XACT_ABORT ON; SET LOCK_TIMEOUT %d", configuration.getLockTimeout()));
        ThreadFactory tf = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("hikari-sqlserver-%d")
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

        String table = configuration.getFlywayTable().orElse("__migrations");
        FluentConfiguration flywayConfiguration = Flyway.configure()
                .table(table)
                .dataSource(dataSource)
                .schemas("data")
                .locations("db/migration_sqlserver")
                .placeholderReplacement(false);
        Flyway flyway = flywayConfiguration.load();
        flyway.migrate();
    }
}

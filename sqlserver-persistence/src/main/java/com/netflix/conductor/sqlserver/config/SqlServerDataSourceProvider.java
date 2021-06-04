/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.sqlserver.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Paths;
import java.util.concurrent.ThreadFactory;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerDataSourceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDataSourceProvider.class);

    private final SqlServerProperties properties;

    public SqlServerDataSourceProvider(SqlServerProperties properties) {
        this.properties = properties;
    }

    public DataSource getDataSource() {
        HikariDataSource dataSource = null;
        try {
            dataSource = new HikariDataSource(createConfiguration());
            flywayMigrate(dataSource);
            return dataSource;
        } catch (final Throwable t) {
            if (null != dataSource && !dataSource.isClosed()) {
                dataSource.close();
            }
            LOGGER.error("error migration DB", t);
            throw t;
        }
    }

    private HikariConfig createConfiguration() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(properties.getJdbcUrl());
        hikariConfig.setUsername(properties.getJdbcUsername());
        hikariConfig.setPassword(properties.getJdbcPassword());
        hikariConfig.setAutoCommit(false);
        hikariConfig.setMaximumPoolSize(properties.getConnectionPoolMaxSize());
        hikariConfig.setMinimumIdle(properties.getConnectionPoolMinIdle());
        hikariConfig.setMaxLifetime(properties.getConnectionMaxLifetime().toMillis());
        hikariConfig.setIdleTimeout(properties.getConnectionIdleTimeout().toMillis());
        hikariConfig.setConnectionTimeout(properties.getConnectionTimeout().toMillis());
        hikariConfig.setTransactionIsolation(properties.getTransactionIsolationLevel());
        hikariConfig.setAutoCommit(properties.isAutoCommit());
        hikariConfig.setSchema("data");

        ThreadFactory tf = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("hikari-postgres-%d")
            .build();

        hikariConfig.setThreadFactory(tf);
        return hikariConfig;
    }

    // TODO Move this into a class that has complete lifecycle for the connection, i.e. startup and shutdown.
    private void flywayMigrate(DataSource dataSource) {
        boolean enabled = properties.isFlywayEnabled();
        if (!enabled) {
            LOGGER.debug("Flyway migrations are disabled");
            return;
        }

        Flyway flyway = new Flyway();
        properties.getFlywayTable().ifPresent(tableName -> {
            LOGGER.debug("Using Flyway migration table '{}'", tableName);
            flyway.setTable(tableName);
        });

        flyway.setDataSource(dataSource);
        flyway.setSchemas("data");
        flyway.setLocations(Paths.get("db","migration_sqlserver").toString());
        flyway.setPlaceholderReplacement(false);
        flyway.migrate();
    }
}

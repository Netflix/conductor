/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.conductor.sqlserver;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.conductor.core.config.Configuration;

public interface SqlServerConfiguration extends Configuration {

    String JDBC_URL_PROPERTY_NAME = "jdbc.url";
    String JDBC_URL_DEFAULT_VALUE = "jdbc:sqlserver://sqlserver:1433;database=Conductor;encrypt=false;trustServerCertificate=true;";

    String JDBC_USER_NAME_PROPERTY_NAME = "jdbc.username";
    String JDBC_USER_NAME_DEFAULT_VALUE = "sa";

    String JDBC_PASSWORD_PROPERTY_NAME = "jdbc.password";
    String JDBC_PASSWORD_DEFAULT_VALUE = "Password1";

    String FLYWAY_ENABLED_PROPERTY_NAME = "flyway.enabled";
    boolean FLYWAY_ENABLED_DEFAULT_VALUE = true;

    String FLYWAY_TABLE_PROPERTY_NAME = "flyway.table";
    Optional<String> FLYWAY_TABLE_DEFAULT_VALUE = Optional.empty();

    // The defaults are currently in line with the HikariConfig defaults, which are unfortunately private.
    String CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME = "conductor.sqlserver.connection.pool.size.max";
    int CONNECTION_POOL_MAX_SIZE_DEFAULT_VALUE = -1;

    String CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME = "conductor.sqlserver.connection.pool.idle.min";
    int CONNECTION_POOL_MINIMUM_IDLE_DEFAULT_VALUE = -1;

    String CONNECTION_MAX_LIFETIME_PROPERTY_NAME = "conductor.sqlserver.connection.lifetime.max";
    long CONNECTION_MAX_LIFETIME_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(30);

    String CONNECTION_IDLE_TIMEOUT_PROPERTY_NAME = "conductor.sqlserver.connection.idle.timeout";
    long CONNECTION_IDLE_TIMEOUT_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(10);

    String CONNECTION_TIMEOUT_PROPERTY_NAME = "conductor.sqlserver.connection.timeout";
    long CONNECTION_TIMEOUT_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(30);

    String LOCK_TIMEOUT_PROPERTY_NAME = "conductor.sqlserver.lock.timeout";
    long LOCK_TIMEOUT_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(30);

    String ISOLATION_LEVEL_PROPERTY_NAME = "conductor.sqlserver.transaction.isolation.level";
    String ISOLATION_LEVEL_DEFAULT_VALUE = "";

    String AUTO_COMMIT_PROPERTY_NAME = "conductor.sqlserver.autocommit";
    // This is consistent with the current default when building the Hikari Client.
    boolean AUTO_COMMIT_DEFAULT_VALUE = false;

    String QUEUE_STRATEGY_PROPERTY_NAME = "conductor.sqlserver.queue.sharding.strategy";
    String QUEUE_STRATEGY_DEFAULT_VALUE = "shared";

    String PROC_REMOVES_INTERVAL_PROPERTY_NAME = "conductor.sqlserver.processRemoves.intervalSeconds";
    long PROC_REMOVES_INTERVAL_DEFAULT_VALUE = 0;

    String PROC_UNACKS_INTERVAL_PROPERTY_NAME = "conductor.sqlserver.processUnacks.intervalMillieconds";
    long PROC_UNACKS_INTERVAL_DEFAULT_VALUE = 60_000L;

    default QUEUE_STRATEGY getQueueStrategy() {
        return QUEUE_STRATEGY.valueOf(getQueueStrategyString());
    }

    default String getQueueStrategyString() {
        return getProperty(QUEUE_STRATEGY_PROPERTY_NAME, QUEUE_STRATEGY_DEFAULT_VALUE).toUpperCase();
    }

    default String getJdbcUrl() {
        return getProperty(JDBC_URL_PROPERTY_NAME, JDBC_URL_DEFAULT_VALUE);
    }

    default String getJdbcUserName() {
        return getProperty(JDBC_USER_NAME_PROPERTY_NAME, JDBC_USER_NAME_DEFAULT_VALUE);
    }

    default String getJdbcPassword() {
        return getProperty(JDBC_PASSWORD_PROPERTY_NAME, JDBC_PASSWORD_DEFAULT_VALUE);
    }

    default boolean isFlywayEnabled() {
        return getBoolProperty(FLYWAY_ENABLED_PROPERTY_NAME, FLYWAY_ENABLED_DEFAULT_VALUE);
    }

    default Optional<String> getFlywayTable() {
        return Optional.ofNullable(getProperty(FLYWAY_TABLE_PROPERTY_NAME, null));
    }

    default int getConnectionPoolMaxSize() {
        return getIntProperty(CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME, CONNECTION_POOL_MAX_SIZE_DEFAULT_VALUE);
    }

    default int getConnectionPoolMinIdle() {
        return getIntProperty(CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME, CONNECTION_POOL_MINIMUM_IDLE_DEFAULT_VALUE);
    }

    default long getConnectionMaxLifetime() {
        return getLongProperty(CONNECTION_MAX_LIFETIME_PROPERTY_NAME, CONNECTION_MAX_LIFETIME_DEFAULT_VALUE);
    }

    default long getConnectionIdleTimeout() {
        return getLongProperty(CONNECTION_IDLE_TIMEOUT_PROPERTY_NAME, CONNECTION_IDLE_TIMEOUT_DEFAULT_VALUE);
    }

    default long getConnectionTimeout() {
        return getLongProperty(CONNECTION_TIMEOUT_PROPERTY_NAME, CONNECTION_TIMEOUT_DEFAULT_VALUE);
    }
    
    default long getLockTimeout() {
        return getLongProperty(LOCK_TIMEOUT_PROPERTY_NAME, LOCK_TIMEOUT_DEFAULT_VALUE);
    }

    default String getTransactionIsolationLevel() {
        return getProperty(ISOLATION_LEVEL_PROPERTY_NAME, ISOLATION_LEVEL_DEFAULT_VALUE);
    }

    default boolean isAutoCommit() {
        return getBoolProperty(AUTO_COMMIT_PROPERTY_NAME, AUTO_COMMIT_DEFAULT_VALUE);
    }

    enum QUEUE_STRATEGY {
        SHARED, LOCAL_ONLY
    }

    default long getProcessAllRemovesInterval() {
        return getLongProperty(PROC_REMOVES_INTERVAL_PROPERTY_NAME, PROC_REMOVES_INTERVAL_DEFAULT_VALUE);
    }

    default long getProcessAllUnacksInterval() {
        return getLongProperty(PROC_REMOVES_INTERVAL_PROPERTY_NAME, PROC_REMOVES_INTERVAL_DEFAULT_VALUE);
    }
}

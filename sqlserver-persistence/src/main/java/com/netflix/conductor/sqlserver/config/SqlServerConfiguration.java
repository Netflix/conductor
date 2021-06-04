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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.grpc.server.GRPCServerProperties;
import com.netflix.conductor.sqlserver.dao.SqlServerExecutionDAO;
import com.netflix.conductor.sqlserver.dao.SqlServerLock;
import com.netflix.conductor.sqlserver.dao.SqlServerMetadataDAO;
import com.netflix.conductor.sqlserver.dao.SqlServerQueueDAO;
import com.netflix.conductor.sqlserver.config.SqlServerDataSourceProvider;

import java.net.UnknownHostException;
import java.sql.SQLException;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SqlServerProperties.class)
@ConditionalOnProperty(name = "conductor.db.type", havingValue = "sqlserver")
public class SqlServerConfiguration {

    @Bean
    public DataSource dataSource(SqlServerProperties config) {
        return new SqlServerDataSourceProvider(config).getDataSource();
    }

    @Bean
    public MetadataDAO sqlServerMetadataDAO(ObjectMapper objectMapper, DataSource dataSource,
        SqlServerProperties properties) {
        return new SqlServerMetadataDAO(objectMapper, dataSource, properties);
    }

    @Bean
    public ExecutionDAO sqlServerExecutionDAO(ObjectMapper objectMapper, DataSource dataSource) {
        return new SqlServerExecutionDAO(objectMapper, dataSource);
    }

    @Bean
    public QueueDAO sqlServerQueueDAO(ObjectMapper objectMapper, DataSource dataSource, SqlServerProperties properties) throws SQLException {
        return new SqlServerQueueDAO(objectMapper, dataSource, properties);
    }

    @Bean
    @ConditionalOnProperty(name = "conductor.workflow-execution-lock.type", havingValue = "sqlserver")
    public Lock provideLock(ObjectMapper objectMapper, DataSource dataSource, 
        SqlServerProperties properties, GRPCServerProperties grpc) throws UnknownHostException {
        return new SqlServerLock(objectMapper, dataSource, properties, grpc);
    }
}

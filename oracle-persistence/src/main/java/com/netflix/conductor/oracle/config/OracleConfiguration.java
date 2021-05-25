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
package com.netflix.conductor.oracle.config;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.oracle.dao.OracleExecutionDAO;
import com.netflix.conductor.oracle.dao.OracleMetadataDAO;
import com.netflix.conductor.oracle.dao.OracleQueueDAO;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OracleProperties.class)
@ConditionalOnProperty(name = "conductor.db.type", havingValue = "oracle")
// Import the DataSourceAutoConfiguration when oracle database is selected.
// By default the datasource configuration is excluded in the main module.
@Import(DataSourceAutoConfiguration.class)
public class OracleConfiguration {
	
	@Bean
    @DependsOn({"flyway"})
    public MetadataDAO oracleMetadataDAO(ObjectMapper objectMapper, DataSource dataSource, OracleProperties properties) {
		return new OracleMetadataDAO(objectMapper, dataSource, properties);
    }

    @Bean
    @DependsOn({"flyway"})
    public ExecutionDAO oracleExecutionDAO(ObjectMapper objectMapper, DataSource dataSource) {
        return new OracleExecutionDAO(objectMapper, dataSource);
    }

    @Bean
    @DependsOn({"flyway"})
    public QueueDAO oracleQueueDAO(ObjectMapper objectMapper, DataSource dataSource) {
        return new OracleQueueDAO(objectMapper, dataSource);
    }
}

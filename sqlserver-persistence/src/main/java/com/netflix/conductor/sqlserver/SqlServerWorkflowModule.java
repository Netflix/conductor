/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.sqlserver;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.dao.sqlserver.SqlServerExecutionDAO;
import com.netflix.conductor.dao.sqlserver.SqlServerMetadataDAO;
import com.netflix.conductor.dao.sqlserver.SqlServerQueueDAO;
import javax.sql.DataSource;

/**
 * @author mustafa
 */
public class SqlServerWorkflowModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SqlServerConfiguration.class).to(SystemPropertiesSqlServerConfiguration.class);
        bind(DataSource.class).toProvider(SqlServerDataSourceProvider.class).in(Scopes.SINGLETON);
        bind(MetadataDAO.class).to(SqlServerMetadataDAO.class);
        bind(EventHandlerDAO.class).to(SqlServerMetadataDAO.class);
        bind(ExecutionDAO.class).to(SqlServerExecutionDAO.class);
        bind(RateLimitingDAO.class).to(SqlServerExecutionDAO.class);
        bind(PollDataDAO.class).to(SqlServerExecutionDAO.class);
        bind(QueueDAO.class).to(SqlServerQueueDAO.class);
    }
}

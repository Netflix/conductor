/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.conductor.tests.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.config.CoreModule;
import com.netflix.conductor.core.config.EventModule;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import com.netflix.conductor.core.execution.WorkflowStatusListenerStub;
import com.netflix.conductor.core.utils.NoopLockModule;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.dao.sqlserver.SqlServerExecutionDAO;
import com.netflix.conductor.dao.sqlserver.SqlServerMetadataDAO;
import com.netflix.conductor.dao.sqlserver.SqlServerQueueDAO;
import com.netflix.conductor.service.MetadataService;
import com.netflix.conductor.service.MetadataServiceImpl;
import com.netflix.conductor.sqlserver.SqlServerConfiguration;
import com.netflix.conductor.sqlserver.SqlServerDataSourceProvider;
import com.netflix.conductor.sqlserver.SystemPropertiesSqlServerConfiguration;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jvemugunta
 */
public class SqlServerTestModule extends AbstractModule {

    private int maxThreads = 50;

    private ExecutorService executorService;

    @Override
    protected void configure() {


        bind(Configuration.class).to(SystemPropertiesSqlServerConfiguration.class).in(Singleton.class);
        bind(SqlServerConfiguration.class).to(SystemPropertiesSqlServerConfiguration.class).in(Singleton.class);

        bind(DataSource.class).toProvider(SqlServerDataSourceProvider.class).in(Scopes.SINGLETON);
        bind(MetadataDAO.class).to(SqlServerMetadataDAO.class);
        bind(EventHandlerDAO.class).to(SqlServerMetadataDAO.class);
        bind(ExecutionDAO.class).to(SqlServerExecutionDAO.class);
        bind(RateLimitingDAO.class).to(SqlServerExecutionDAO.class);
        bind(PollDataDAO.class).to(SqlServerExecutionDAO.class);
        bind(QueueDAO.class).to(SqlServerQueueDAO.class);
        bind(IndexDAO.class).to(MockIndexDAO.class);
        bind(WorkflowStatusListener.class).to(WorkflowStatusListenerStub.class);

        install(new CoreModule());
        install(new EventModule());
        bind(UserTask.class).asEagerSingleton();
        bind(ObjectMapper.class).toProvider(JsonMapperProvider.class);
        bind(ExternalPayloadStorage.class).to(MockExternalPayloadStorage.class);

        bind(MetadataService.class).to(MetadataServiceImpl.class);
        install(new NoopLockModule());
    }


    @Provides
    public ExecutorService getExecutorService() {
        return this.executorService;
    }

    private void configureExecutorService() {
        AtomicInteger count = new AtomicInteger(0);
        this.executorService = java.util.concurrent.Executors.newFixedThreadPool(maxThreads, runnable -> {
            Thread workflowWorkerThread = new Thread(runnable);
            workflowWorkerThread.setName(String.format("workflow-worker-%d", count.getAndIncrement()));
            return workflowWorkerThread;
        });
    }
}

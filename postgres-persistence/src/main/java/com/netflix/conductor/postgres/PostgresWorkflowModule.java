package com.netflix.conductor.postgres;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.postgres.PostgresExecutionDAO;
import com.netflix.conductor.dao.postgres.PostgresMetadataDAO;
import com.netflix.conductor.dao.postgres.PostgresQueueDAO;
import com.netflix.conductor.sql.SQLConfiguration;
import com.netflix.conductor.sql.SystemPropertiesSQLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class PostgresWorkflowModule extends AbstractModule {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void configure() {
        bind(SQLConfiguration.class).to(SystemPropertiesSQLConfiguration.class).in(Singleton.class);
        bind(DataSource.class).toProvider(PostgresDatasourceProvider.class).in(Singleton.class);
        bind(MetadataDAO.class).to(PostgresMetadataDAO.class);
        bind(ExecutionDAO.class).to(PostgresExecutionDAO.class);
        bind(QueueDAO.class).to(PostgresQueueDAO.class);
    }

}
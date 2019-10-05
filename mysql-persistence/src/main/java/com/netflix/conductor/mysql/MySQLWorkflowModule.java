package com.netflix.conductor.mysql;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.mysql.MySQLExecutionDAO;
import com.netflix.conductor.dao.mysql.MySQLMetadataDAO;
import com.netflix.conductor.dao.mysql.MySQLQueueDAO;
import com.netflix.conductor.sql.SQLConfiguration;
import com.netflix.conductor.sql.SystemPropertiesSQLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * @author mustafa
 */
public class MySQLWorkflowModule extends AbstractModule {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void configure() {
        bind(SQLConfiguration.class).to(SystemPropertiesSQLConfiguration.class).in(Singleton.class);
        bind(DataSource.class).toProvider(MySQLDatasourceProvider.class).in(Singleton.class);
        bind(MetadataDAO.class).to(MySQLMetadataDAO.class);
        bind(ExecutionDAO.class).to(MySQLExecutionDAO.class);
        bind(QueueDAO.class).to(MySQLQueueDAO.class);
    }

}
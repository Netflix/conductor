package com.netflix.conductor.dao.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.sql.SQLMetadataDAO;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

@Singleton
public class MySQLMetadataDAO extends SQLMetadataDAO {

    @Inject
    public MySQLMetadataDAO(ObjectMapper om, DataSource dataSource, Configuration config) {
        super(om, dataSource,config);
    }

}

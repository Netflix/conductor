package com.netflix.conductor.aurora;

import com.netflix.conductor.core.config.Configuration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.sql.DataSource;

public class AuroraDataSourceProvider implements Provider<DataSource> {
    private Configuration config;

    @Inject
    public AuroraDataSourceProvider(Configuration config) {
        this.config = config;
    }

    @Override
    public DataSource get() {
        String db = config.getProperty("aurora.db", null);
        String host = config.getProperty("aurora.host", null);
        String port = config.getProperty("aurora.port", "5432");
        String user = config.getProperty("aurora.user", null);
        String pwd = config.getProperty("aurora.password", null);

        String options = config.getProperty("aurora.options","");
        String url = String.format("jdbc:postgresql://%s:%s/%s?%s", host, port, db, options);

        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setJdbcUrl(url);
        poolConfig.setUsername(user);
        poolConfig.setPassword(pwd);
        poolConfig.setAutoCommit(true);

        return new HikariDataSource(poolConfig);
    }
}

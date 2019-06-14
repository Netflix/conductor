package com.netflix.conductor.aurora;

import com.netflix.conductor.core.config.Configuration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.inject.Inject;
import javax.inject.Provider;

public class AuroraDataSourceProvider implements Provider<HikariDataSource> {
    private Configuration config;

    @Inject
    public AuroraDataSourceProvider(Configuration config) {
        this.config = config;
    }

    @Override
    public HikariDataSource get() {
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
        poolConfig.setAutoCommit(false);
        poolConfig.setMaximumPoolSize(1000);
        poolConfig.setConnectionTimeout(60_000);

        return new HikariDataSource(poolConfig);
    }
}

package com.netflix.conductor.dao.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.config.TestConfiguration;
import com.zaxxer.hikari.HikariDataSource;

public interface SqlDAOTestUtil {
    HikariDataSource getDataSource();
    ObjectMapper getObjectMapper();
    TestConfiguration getTestConfiguration();
    void resetAllData();
}

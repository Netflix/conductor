package com.netflix.conductor.dao.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.sql.SqlDAOTestUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


@SuppressWarnings("Duplicates")
public class PostgresDAOTestUtil implements SqlDAOTestUtil {
    private static final Logger logger = LoggerFactory.getLogger(PostgresDAOTestUtil.class);
    private final HikariDataSource dataSource;
    private final TestConfiguration testConfiguration = new TestConfiguration();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();
    private static final String JDBC_URL_PREFIX = "jdbc:postgresql://localhost:54320/";

    PostgresDAOTestUtil(String dbName) throws Exception {

        DataSource ds = EmbeddedDatabase.INSTANCE.getDataSource();

        createDb(ds, dbName);

        testConfiguration.setProperty("jdbc.url", JDBC_URL_PREFIX + dbName);
        testConfiguration.setProperty("jdbc.username", "postgres");
        testConfiguration.setProperty("jdbc.password", "postgres");

        this.dataSource = getDataSource(testConfiguration);
    }

    private HikariDataSource getDataSource(Configuration config) {

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.getProperty("jdbc.url", JDBC_URL_PREFIX + "conductor"));
        dataSource.setUsername(config.getProperty("jdbc.username", "postgres"));
        dataSource.setPassword(config.getProperty("jdbc.password", "postgres"));
        dataSource.setAutoCommit(false);

        // Prevent DB from getting exhausted during rapid testing
        dataSource.setMaximumPoolSize(8);

        flywayMigrate(dataSource);

        return dataSource;
    }

    private void flywayMigrate(DataSource dataSource) {

        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setPlaceholderReplacement(false);
        flyway.migrate();
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public TestConfiguration getTestConfiguration() {
        return testConfiguration;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public static void createDb(DataSource ds, String dbName) {
        exec(ds, dbName,"CREATE","");
    }

    public static void dropDb(DataSource ds, String dbName) {
        exec(ds, dbName, "DROP","IF EXISTS");
    }

    private static void exec(DataSource ds, String dbName, String prefix, String suffix) {

        try (Connection connection = ds.getConnection()) {
            String stmt = String.format("%s DATABASE %s %s", prefix, suffix, dbName);
            try (PreparedStatement ps = connection.prepareStatement(stmt)) {
                ps.executeUpdate();
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    public void resetAllData() {
        logger.info("Resetting data for test");
        dropDb(EmbeddedDatabase.INSTANCE.getDataSource(),"conductor");
        flywayMigrate(dataSource);
    }
}

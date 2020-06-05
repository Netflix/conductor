package com.netflix.conductor.tests.integration;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.conductor.bootstrap.BootstrapModule;
import com.netflix.conductor.bootstrap.ModulesProvider;
import com.netflix.conductor.client.grpc.MetadataClient;
import com.netflix.conductor.client.grpc.TaskClient;
import com.netflix.conductor.client.grpc.WorkflowClient;
import com.netflix.conductor.dao.postgres.EmbeddedDatabase;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.grpc.server.GRPCServer;
import com.netflix.conductor.grpc.server.GRPCServerProvider;
import com.netflix.conductor.tests.utils.TestEnvironment;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static com.netflix.conductor.core.config.Configuration.DB_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_URL_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.EMBEDDED_PORT_PROPERTY_NAME;
import static com.netflix.conductor.grpc.server.GRPCServerConfiguration.ENABLED_PROPERTY_NAME;
import static com.netflix.conductor.grpc.server.GRPCServerConfiguration.PORT_PROPERTY_NAME;
import static com.netflix.conductor.postgres.PostgresConfiguration.*;
import static org.junit.Assert.assertTrue;

public class PostgresGrpcEndToEndTest extends AbstractGrpcEndToEndTest {

    private static final int SERVER_PORT = 8098;
    private static final EmbeddedDatabase DB = EmbeddedDatabase.INSTANCE;
    protected static Optional<GRPCServer> server;

    private static final String JDBC_URL = "jdbc:postgresql://localhost:54320/conductor";
    private static HikariDataSource dataSource;

    @BeforeClass
    public static void setup() throws Exception {
        TestEnvironment.setup();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername("postgres");
        config.setPassword("postgres");
        config.setAutoCommit(true);

        dataSource = new HikariDataSource(config);

        // pre-populate the database, to test baseline.on.migrate feature

        String migrationsDir = "../postgres-persistence/src/main/resources/db/migration_conductor/postgres";

        executeSQL(migrationsDir + "/V1__initial_schema.sql", dataSource.getConnection());

        System.setProperty("workflow.namespace.prefix", "conductor" + System.getProperty("user.name"));
        System.setProperty(DB_PROPERTY_NAME, "postgres");
        System.setProperty(ENABLED_PROPERTY_NAME, "true");
        System.setProperty(PORT_PROPERTY_NAME, "8098");
        System.setProperty(EMBEDDED_PORT_PROPERTY_NAME, "9208");
        System.setProperty(ELASTIC_SEARCH_URL_PROPERTY_NAME, "localhost:9308");
        System.setProperty(JDBC_URL_PROPERTY_NAME, JDBC_URL);
        System.setProperty(JDBC_USER_NAME_PROPERTY_NAME, "postgres");
        System.setProperty(JDBC_PASSWORD_PROPERTY_NAME, "postgres");

        System.setProperty(CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME, "8");
        System.setProperty(CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME, "8");
        System.setProperty(CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME, "300000");

        System.setProperty(FLYWAY_ENABLED_PROPERTY_NAME, "true");
        System.setProperty(FLYWAY_BASELINE_PROPERTY_NAME, "true");

        // This indicates that database has already been initialized with V1 tables
        // With this set, flyway will mark the schema as version 1 as the baseline
        // If flyway is later migrated, all migrations *after* V1 will be applied
        System.setProperty(FLYWAY_BASELINE_VERSION_PROPERTY_NAME, "1");
        System.setProperty(FLYWAY_LOCATIONS_PROPERTY_NAME, "filesystem:" + migrationsDir);

        Injector bootInjector = Guice.createInjector(new BootstrapModule());
        Injector serverInjector = Guice.createInjector(bootInjector.getInstance(ModulesProvider.class).get());

        search = serverInjector.getInstance(EmbeddedElasticSearchProvider .class).get().get();
        search.start();

        server = serverInjector.getInstance(GRPCServerProvider.class).get();
        assertTrue("failed to instantiate GRPCServer", server.isPresent());
        server.get().start();

        taskClient = new TaskClient("localhost", SERVER_PORT);
        workflowClient = new WorkflowClient("localhost", SERVER_PORT);
        metadataClient = new MetadataClient("localhost", SERVER_PORT);
    }

    @Test
    public void testSchemaVersion() {
        // ensure flyway schema_version was incremented
        int numVers = 0;
        try (Statement stmt = dataSource.getConnection().createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM schema_version WHERE version::INTEGER > 1");
            while (rs.next()) {
               numVers++;
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertTrue(numVers > 0);
    }

    @AfterClass
    public static void teardown() throws Exception {
        TestEnvironment.teardown();
        search.stop();
        server.ifPresent(GRPCServer::stop);
        DB.getDataSource().getConnection().close();
    }

}
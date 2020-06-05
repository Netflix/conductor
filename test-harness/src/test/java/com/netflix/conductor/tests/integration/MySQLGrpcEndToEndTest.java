/**
 * Copyright 2016 Netflix, Inc.
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
/**
 *
 */
package com.netflix.conductor.tests.integration;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.conductor.bootstrap.BootstrapModule;
import com.netflix.conductor.bootstrap.ModulesProvider;
import com.netflix.conductor.client.grpc.MetadataClient;
import com.netflix.conductor.client.grpc.TaskClient;
import com.netflix.conductor.client.grpc.WorkflowClient;
import com.netflix.conductor.dao.mysql.EmbeddedDatabase;
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

import static com.netflix.conductor.core.config.Configuration.EXECUTION_LOCK_ENABLED_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_URL_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.EMBEDDED_PORT_PROPERTY_NAME;
import static com.netflix.conductor.grpc.server.GRPCServerConfiguration.ENABLED_PROPERTY_NAME;
import static com.netflix.conductor.grpc.server.GRPCServerConfiguration.PORT_PROPERTY_NAME;
import static com.netflix.conductor.mysql.MySQLConfiguration.*;
import static org.junit.Assert.assertTrue;

/**
 * @author Viren
 */
public class MySQLGrpcEndToEndTest extends AbstractGrpcEndToEndTest {

    private static final int SERVER_PORT = 8094;
    private static final EmbeddedDatabase DB = EmbeddedDatabase.INSTANCE;
    protected static Optional<GRPCServer> server;

    public static final String JDBC_URL = "jdbc:mysql://localhost:33307/conductor?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static HikariDataSource dataSource;

    @BeforeClass
    public static void setup() throws Exception {
        TestEnvironment.setup();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername("root");
        config.setPassword("");
        config.setAutoCommit(true);

        dataSource = new HikariDataSource(config);

        // pre-populate the database, to test baseline.on.migrate feature

        String migrationsDir = "../mysql-persistence/src/main/resources/db/migration_conductor/mysql";

        executeSQL(migrationsDir + "/V1__initial_schema.sql", dataSource.getConnection());

        System.setProperty("workflow.namespace.prefix", "conductor" + System.getProperty("user.name"));
        System.setProperty(DB_PROPERTY_NAME, "mysql");
        System.setProperty(ENABLED_PROPERTY_NAME, "true");
        System.setProperty(PORT_PROPERTY_NAME, "8094");
        System.setProperty(EMBEDDED_PORT_PROPERTY_NAME, "9204");
        System.setProperty(ELASTIC_SEARCH_URL_PROPERTY_NAME, "localhost:9304");
        System.setProperty("jdbc.url", JDBC_URL);
        System.setProperty("jdbc.username", "root");
        System.setProperty("jdbc.password", "");

        System.setProperty("conductor.mysql.connection.pool.size.min", "8");
        System.setProperty("conductor.mysql.connection.pool.size.max", "8");
        System.setProperty("conductor.mysql.connection.pool.idle.min", "300000");

        System.setProperty(FLYWAY_ENABLED_PROPERTY_NAME, "true");
        System.setProperty(FLYWAY_BASELINE_PROPERTY_NAME, "true");

        // This indicates that database has already been initialized with V1 tables
        // With this set, flyway will mark the schema as version 1 as the baseline
        // If flyway is later migrated, all migrations *after* V1 will be applied
        System.setProperty(FLYWAY_BASELINE_VERSION_PROPERTY_NAME, "1");
        System.setProperty(FLYWAY_LOCATIONS_PROPERTY_NAME, "filesystem:" + migrationsDir);

        System.setProperty("conductor.workflow.input.payload.threshold.kb", "10");
        System.setProperty("conductor.max.workflow.input.payload.threshold.kb", "10240");
        System.setProperty("conductor.workflow.output.payload.threshold.kb", "10");
        System.setProperty("conductor.max.workflow.output.payload.threshold.kb", "10240");
        System.setProperty("conductor.task.input.payload.threshold.kb", "1");
        System.setProperty("conductor.max.task.input.payload.threshold.kb", "10240");
        System.setProperty("conductor.task.output.payload.threshold.kb", "10");
        System.setProperty("conductor.max.task.output.payload.threshold.kb", "10240");

        System.setProperty(EXECUTION_LOCK_ENABLED_PROPERTY_NAME, "false");

        Injector bootInjector = Guice.createInjector(new BootstrapModule());
        Injector serverInjector = Guice.createInjector(bootInjector.getInstance(ModulesProvider.class).get());

        search = serverInjector.getInstance(EmbeddedElasticSearchProvider.class).get().get();
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
            ResultSet rs = stmt.executeQuery("SELECT * FROM schema_version WHERE CAST(version AS INTEGER) > 1");
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
        DB.getDB().stop();
    }

}

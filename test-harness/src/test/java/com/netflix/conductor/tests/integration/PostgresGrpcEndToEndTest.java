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
import com.netflix.conductor.dao.postgres.EmbeddedDatabase;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.grpc.server.GRPCServerProvider;
import com.netflix.conductor.tests.utils.TestEnvironment;
import org.junit.BeforeClass;

import static com.netflix.conductor.core.config.Configuration.DB_PROPERTY_NAME;
import static com.netflix.conductor.core.config.Configuration.WORKFLOW_NAMESPACE_PREFIX_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.ELASTIC_SEARCH_URL_PROPERTY_NAME;
import static com.netflix.conductor.elasticsearch.ElasticSearchConfiguration.EMBEDDED_PORT_PROPERTY_NAME;
import static com.netflix.conductor.grpc.server.GRPCServerConfiguration.ENABLED_PROPERTY_NAME;
import static com.netflix.conductor.grpc.server.GRPCServerConfiguration.PORT_PROPERTY_NAME;
import static com.netflix.conductor.sql.SQLConfiguration.JDBC_PASSWORD_PROPERTY_NAME;
import static com.netflix.conductor.sql.SQLConfiguration.JDBC_URL_PROPERTY_NAME;
import static com.netflix.conductor.sql.SQLConfiguration.JDBC_USER_NAME_PROPERTY_NAME;
import static org.junit.Assert.assertTrue;

public class PostgresGrpcEndToEndTest extends AbstractGrpcEndToEndTest {

  private static final int SERVER_PORT = 8098;
  private static final EmbeddedDatabase DB = EmbeddedDatabase.INSTANCE;

  @BeforeClass
  public static void setup() throws Exception {
    TestEnvironment.setup();

    System.setProperty(WORKFLOW_NAMESPACE_PREFIX_PROPERTY_NAME, "conductor" + System.getProperty("user.name"));
    System.setProperty(DB_PROPERTY_NAME, "postgres");
    System.setProperty(ENABLED_PROPERTY_NAME, "true");
    System.setProperty(PORT_PROPERTY_NAME, "8098");
    System.setProperty(EMBEDDED_PORT_PROPERTY_NAME, "9208");
    System.setProperty(ELASTIC_SEARCH_URL_PROPERTY_NAME, "localhost:9308");
    System.setProperty(JDBC_URL_PROPERTY_NAME, "jdbc:postgresql://localhost:54320/conductor");
    System.setProperty(JDBC_USER_NAME_PROPERTY_NAME, "postgres");
    System.setProperty(JDBC_PASSWORD_PROPERTY_NAME, "postgres");

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

}

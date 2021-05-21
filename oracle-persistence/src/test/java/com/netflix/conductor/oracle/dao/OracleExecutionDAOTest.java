/*
 *  Copyright 2021 Netflix, Inc.
 *  <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.oracle.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.ExecutionDAOTest;
import com.netflix.conductor.oracle.util.OracleDAOTestUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@ContextConfiguration(classes = {TestObjectMapperConfiguration.class})
@RunWith(SpringRunner.class)
public class OracleExecutionDAOTest extends ExecutionDAOTest {

    private OracleDAOTestUtil testUtil;
    private OracleExecutionDAO executionDAO;

    @Autowired
    private ObjectMapper objectMapper;

    @Rule
    public TestName name = new TestName();

    @SuppressWarnings("deprecation")
	@ClassRule
    public static OracleContainer oracleContainer = new OracleContainer();

    @BeforeAll
    public static void startup() {
        oracleContainer.start();
    }

    @TestConfiguration
        static class OracleTestConfiguration {

            @Bean
            DataSource dataSource() {
                HikariConfig hikariConfig = new HikariConfig();
                hikariConfig.setJdbcUrl(oracleContainer.getJdbcUrl());
                hikariConfig.setUsername(oracleContainer.getUsername());
                hikariConfig.setPassword(oracleContainer.getPassword());

                return new HikariDataSource(hikariConfig);
            }
      }

    @Before
    public void setup() {
       // oracleContainer = new OracleContainer(DockerImageName.parse("oracle")).withDatabaseName(name.getMethodName());
        oracleContainer.start();
        testUtil = new OracleDAOTestUtil(oracleContainer, objectMapper);
        executionDAO = new OracleExecutionDAO(testUtil.getObjectMapper(), testUtil.getDataSource());
    }

    @After
    public void teardown() {
        testUtil.getDataSource().close();
    }

    @Test
    public void testPendingByCorrelationId() {

        WorkflowDef def = new WorkflowDef();
        def.setName("pending_count_correlation_jtest");

        Workflow workflow = createTestWorkflow();
        workflow.setWorkflowDefinition(def);

        generateWorkflows(workflow, 10);

        List<Workflow> bycorrelationId = getExecutionDAO()
            .getWorkflowsByCorrelationId("pending_count_correlation_jtest", "corr001", true);
        assertNotNull(bycorrelationId);
        assertEquals(10, bycorrelationId.size());
    }

    @Override
    public ExecutionDAO getExecutionDAO() {
        return executionDAO;
    }
}

/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.conductor.test.integration.grpc.sqlserver;

import com.netflix.conductor.client.grpc.MetadataClient;
import com.netflix.conductor.client.grpc.TaskClient;
import com.netflix.conductor.client.grpc.WorkflowClient;
import com.netflix.conductor.test.integration.grpc.AbstractGrpcEndToEndTest;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@TestPropertySource(properties = {
    "conductor.db.type=sqlserver",
    "conductor.grpc-server.port=8098",
    "conductor.sqlserver.jdbcUrl=jdbc:tc:sqlserver://sqlserver:1433;database=Conductor;encrypt=false;trustServerCertificate=true;", // "tc" prefix starts the Postgres container
    "conductor.sqlserver.jdbcUsername=sa",
    "conductor.sqlserver.jdbcPassword=Password1",
    "conductor.sqlserver.connectionPoolMaxSize=8",
    "conductor.sqlserver.connectionPoolMinIdle=300000",
    "spring.flyway.enabled=false"
})
public class SqlServerGrpcEndToEndTest extends AbstractGrpcEndToEndTest {

    @Before
    public void init() {
        taskClient = new TaskClient("localhost", 8104);
        workflowClient = new WorkflowClient("localhost", 8104);
        metadataClient = new MetadataClient("localhost", 8104);
    }
}

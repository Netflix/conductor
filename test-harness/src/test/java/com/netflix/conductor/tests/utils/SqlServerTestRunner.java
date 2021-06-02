/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.tests.utils;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.runners.BlockJUnit4ClassRunner;

/**
 * @author Viren
 *
 */
public class SqlServerTestRunner extends BlockJUnit4ClassRunner {

    private Injector injector;

    static {
        System.setProperty("db", "sqlserver");

        System.setProperty("EC2_REGION", "us-east-1");
        System.setProperty("EC2_AVAILABILITY_ZONE", "us-east-1c");

        System.setProperty("conductor.workflow.input.payload.threshold.kb", "10");
        System.setProperty("conductor.max.workflow.input.payload.threshold.kb", "10240");
        System.setProperty("conductor.workflow.output.payload.threshold.kb", "10");
        System.setProperty("conductor.max.workflow.output.payload.threshold.kb", "10240");
        System.setProperty("conductor.task.input.payload.threshold.kb", "1");
        System.setProperty("conductor.max.task.input.payload.threshold.kb", "10240");
        System.setProperty("conductor.task.output.payload.threshold.kb", "10");
        System.setProperty("conductor.max.task.output.payload.threshold.kb", "10240");
        
        // jdbc properties

        System.setProperty("jdbc.url", "jdbc:sqlserver://localhost:1433;database=conductor;encrypt=false;trustServerCertificate=true;");
        System.setProperty("jdbc.username", "SA");
        System.setProperty("jdbc.password", "Password1");
        System.setProperty("conductor.sqlserver.connection.pool.size.min", "8");
        System.setProperty("conductor.sqlserver.connection.pool.size.max", "8");
        System.setProperty("conductor.sqlserver.connection.pool.idle.min", "300000");

    }

    public SqlServerTestRunner(Class<?> klass) throws Exception {
        super(klass);
        System.setProperty("workflow.namespace.prefix", "conductor" + System.getProperty("user.name"));
        injector = Guice.createInjector(new SqlServerTestModule());
    }

    @Override
    protected Object createTest() throws Exception {
        Object test = super.createTest();
        injector.injectMembers(test);
        return test;
    }
}
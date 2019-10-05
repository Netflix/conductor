/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.dao.mysql;

import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.sql.SQLExecutionDAOTest;
import com.netflix.conductor.dao.sql.SqlDAOTestUtil;

@SuppressWarnings("Duplicates")
public class MySQLExecutionDAOTest extends SQLExecutionDAOTest {

    @Override
    protected SqlDAOTestUtil getSqlUtil() throws Exception {
        return new MySQLDAOTestUtil(name.getMethodName().toLowerCase());
    }

    @Override
    protected ExecutionDAO getExecutionDao(SqlDAOTestUtil sqlUtil) {
        return new MySQLExecutionDAO(
          sqlUtil.getObjectMapper(),
          sqlUtil.getDataSource()
        );
    }
}

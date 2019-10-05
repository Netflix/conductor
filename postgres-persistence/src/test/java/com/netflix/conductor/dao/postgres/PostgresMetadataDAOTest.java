package com.netflix.conductor.dao.postgres;

import com.netflix.conductor.dao.sql.SQLMetadataDAOTest;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings("Duplicates")
@RunWith(JUnit4.class)
public class PostgresMetadataDAOTest extends SQLMetadataDAOTest {

    @Before
    public void setup() throws Exception {
        testUtil = new PostgresDAOTestUtil(name.getMethodName().toLowerCase());
        dao = new PostgresMetadataDAO(testUtil.getObjectMapper(), testUtil.getDataSource(), testUtil.getTestConfiguration());
    }

}

package com.netflix.conductor.dao.mysql;

import com.netflix.conductor.dao.sql.SQLMetadataDAOTest;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings("Duplicates")
@RunWith(JUnit4.class)
public class MySQLMetadataDAOTest extends SQLMetadataDAOTest {

    @Before
    public void setup() throws Exception {
        testUtil = new MySQLDAOTestUtil(name.getMethodName().toLowerCase());
        dao = new MySQLMetadataDAO(testUtil.getObjectMapper(), testUtil.getDataSource(), testUtil.getTestConfiguration());
    }

}

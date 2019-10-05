package com.netflix.conductor.dao.mysql;

import com.netflix.conductor.dao.sql.SQLQueueDAOTest;
import org.junit.Before;

@SuppressWarnings("Duplicates")
public class MySQLQueueDAOTest extends SQLQueueDAOTest {

	@Before
	public void setup() throws Exception {
        testUtil = new MySQLDAOTestUtil(name.getMethodName().toLowerCase());
		    dao = new MySQLQueueDAO(testUtil.getObjectMapper(), testUtil.getDataSource());
	}

}

package com.netflix.conductor.dao.postgres;

import com.netflix.conductor.dao.sql.SQLQueueDAOTest;
import org.junit.Before;

@SuppressWarnings("Duplicates")
public class PostgresQueueDAOTest extends SQLQueueDAOTest {

	@Before
	public void setup() throws Exception {
        testUtil = new PostgresDAOTestUtil(name.getMethodName().toLowerCase());
		    dao = new PostgresQueueDAO(testUtil.getObjectMapper(), testUtil.getDataSource());
	}

}

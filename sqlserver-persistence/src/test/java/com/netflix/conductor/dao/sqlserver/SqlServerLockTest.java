package com.netflix.conductor.dao.sqlserver;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.config.ObjectMapperConfiguration;
import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.grpc.server.GRPCServerProperties;
import com.netflix.conductor.sqlserver.config.SqlServerProperties;
import com.netflix.conductor.sqlserver.dao.SqlServerLock;
import com.netflix.conductor.sqlserver.dao.SqlServerQueueDAO;

@ContextConfiguration(classes = {TestObjectMapperConfiguration.class})
@RunWith(SpringRunner.class)
public class SqlServerLockTest {

    private SqlServerDAOTestUtil testUtil;
    private Lock lock;

    @Rule
    public ExpectedException expected = ExpectedException.none();
 
    @Rule
    public TestName name = new TestName();

    public MSSQLServerContainer<?> container;

    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void setup() throws Exception {
        container = new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server")).acceptLicense();
        container.start();
        testUtil = new SqlServerDAOTestUtil(container, objectMapper, name.getMethodName());

        GRPCServerProperties grpc = mock(GRPCServerProperties.class);
        when(grpc.getPort()).thenReturn(8080);
        lock = new SqlServerLock(testUtil.getObjectMapper(), testUtil.getDataSource(), testUtil.getTestProperties(), grpc);
    }

    @After
    public void teardown() throws Exception {
        testUtil.resetAllData();
        testUtil.getDataSource().close();
        container.close();
    }

    @Test
    public void testLocking() {
        String lockId = UUID.randomUUID().toString();
        assertTrue(lock.acquireLock(lockId, 1000, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testLockExpiration() throws InterruptedException {
        String lockId = UUID.randomUUID().toString();

        boolean isLocked = lock.acquireLock(lockId, 1000, 1000, TimeUnit.MILLISECONDS);
        assertTrue("initial acquire", isLocked);

        Thread.sleep(500);

        isLocked = lock.acquireLock(lockId, 100, 1, TimeUnit.MILLISECONDS);
        assertTrue("after 500ms acquire", isLocked);

        Thread.sleep(1100);
        
        isLocked = lock.acquireLock(lockId, 1000, 1, TimeUnit.MILLISECONDS);
        assertTrue("after 1600ms acquire", isLocked);
    }

    @Test
    public void testLockReentry() throws InterruptedException {
        String lockId = UUID.randomUUID().toString();
        boolean isLocked = lock.acquireLock(lockId, 1000, 60000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        Thread.sleep(1000);

        // get the lock back
        isLocked = lock.acquireLock(lockId, 1000, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }


    @Test
    public void testReleaseLock() {
        String lockId = UUID.randomUUID().toString();

        boolean isLocked = lock.acquireLock(lockId, 1000, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        lock.releaseLock(lockId);
    }

    @Test
    public void testGetLockId() throws Exception
    {
        SqlServerProperties p = testUtil.getTestProperties();
        when(p.getLockNamespace()).thenReturn("asdf");
        SqlServerLock lck = new SqlServerLock(null, null, p, mock(GRPCServerProperties.class));
        assertEquals("asdf.qwerty", lck.getLockId("qwerty"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDangerousApi() {
        lock.acquireLock("lockId");
    }
}

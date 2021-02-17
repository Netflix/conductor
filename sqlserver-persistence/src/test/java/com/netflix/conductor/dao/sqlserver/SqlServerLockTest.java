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
import java.util.concurrent.TimeUnit;

import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.utils.Lock;
import com.netflix.conductor.sqlserver.SqlServerConfiguration;

@RunWith(Parameterized.class)
public class SqlServerLockTest {

    private SqlServerDAOTestUtil testUtil;
    private Lock lock;

    @Rule
    public ExpectedException expected = ExpectedException.none();
 
    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        lock = new SqlServerLock(testUtil.getObjectMapper(), testUtil.getDataSource(), testUtil.getTestConfiguration());
    }

    @After
    public void teardown() throws Exception {
        testUtil.resetAllData();
        testUtil.getDataSource().close();
    }

    public SqlServerLockTest(boolean isInMemory) throws Exception {
        testUtil = new SqlServerDAOTestUtil(name.getMethodName());
		if (isInMemory) {
            testUtil.configureInMemoryTableForLock();
        }
    }

    @Parameterized.Parameters
	public static Collection primeNumbers() {
	   return Arrays.asList(new Object[][] {
		  { false },
		  { true },
	   });
	}

    @Test
    public void testLocking() {
        String lockId = "testLocking";
        assertTrue(lock.acquireLock(lockId, 1000, 1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testLockExpiration() throws InterruptedException {
        String lockId = "testLockExpiration";

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
        String lockId = "testLockReentry";
        boolean isLocked = lock.acquireLock(lockId, 1000, 60000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        Thread.sleep(1000);

        // get the lock back
        isLocked = lock.acquireLock(lockId, 1000, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }


    @Test
    public void testReleaseLock() {
        String lockId = "testReleaseLock";

        boolean isLocked = lock.acquireLock(lockId, 1000, 10000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);

        lock.releaseLock(lockId);
    }

    @Test
    public void testGetLockId() throws Exception
    {
        TestConfiguration cnf = new TestConfiguration();
        cnf.setProperty("workflow.decider.locking.namespace", "asdf");
        SqlServerLock lck = new SqlServerLock(null, null, cnf);
        assertEquals("asdf.qwerty", lck.getLockId("qwerty"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDangerousApi() {
        lock.acquireLock("lockId");
    }

    // Can't get static mocks to work
    // @Test
    // public void testGetHolderId() throws Exception {
    //     TestConfiguration cnf = new TestConfiguration();
    //     cnf.setProperty("LOCAL_RACK", "rack");
    //     cnf.setProperty("conductor.jetty.server.port", "8080");
        

    //     InetAddress hostAddr = mock(InetAddress.class);
    //     when(hostAddr.getHostName()).thenReturn("host");

    //     InetAddress throwAddr = mock(InetAddress.class);
    //     when(throwAddr.getHostName()).thenThrow(UnknownHostException.class);
    //     when(throwAddr.getHostAddress()).thenReturn("1.1.1.1");

    //     try(MockedStatic<InetAddress> theMock = Mockito.mockStatic(InetAddress.class)) {
    //         theMock.when(() -> InetAddress.getLocalHost())
    //             .thenReturn(hostAddr);
    //         SqlServerLock lck = new SqlServerLock(null, null, cnf);
    //         assertEquals(
    //             String.format("%s%c%d", "host:8080:rack", '-', Thread.currentThread().getId()), 
    //             lck.getHolderId()       
    //         );
    //     }

    //     try(MockedStatic<InetAddress> theMock = Mockito.mockStatic(InetAddress.class)) {
    //         theMock.when(() -> InetAddress.getLocalHost())
    //             .thenReturn(throwAddr);
    //         SqlServerLock lck = new SqlServerLock(null, null, cnf);
    //         assertEquals(
    //             String.format("%s%c%d", "1.1.1.1:8080:rack", '-', Thread.currentThread().getId()), 
    //             lck.getHolderId()       
    //         );
    //     }
    // }
}

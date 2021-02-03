package com.netflix.conductor.dao.sqlserver;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import com.netflix.conductor.core.utils.Lock;

@RunWith(JUnit4.class)
public class SqlServerLockTest {

    private SqlServerDAOTestUtil testUtil;
    private Lock lock;

    @Rule
    public ExpectedException expected = ExpectedException.none();
 
    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        testUtil = new SqlServerDAOTestUtil(name.getMethodName());
        lock = new SqlServerLock(testUtil.getObjectMapper(), testUtil.getDataSource(), testUtil.getTestConfiguration());
    }

    @After
    public void teardown() throws Exception {
        testUtil.resetAllData();
        testUtil.getDataSource().close();
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
}

/*
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
package com.netflix.conductor.dao.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.utils.Lock;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
        lock = new SqlServerLock(testUtil.getObjectMapper(), testUtil.getDataSource(), testUtil.getTestConfiguration());
    }

    @After
    public void teardown() throws Exception {
        testUtil.resetAllData();
        testUtil.getDataSource().close();
    }

    public SqlServerLockTest() throws Exception {
        testUtil = new SqlServerDAOTestUtil(name.getMethodName());
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
        TestConfiguration cnf = new TestConfiguration();
        cnf.setProperty("workflow.decider.locking.namespace", "asdf");
        SqlServerLock lck = new SqlServerLock(null, null, cnf);
        assertEquals("asdf.qwerty", lck.getLockId("qwerty"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDangerousApi() {
        lock.acquireLock("lockId");
    }
}

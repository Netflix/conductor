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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.netflix.conductor.core.utils.Lock;
import com.netflix.conductor.sqlserver.SqlServerConfiguration;

public class SqlServerLock extends SqlServerBaseDAO implements Lock {

    private String LOCK_NAMESPACE = "";
    private String INSTANCE_IDENTIFIER = "";

    @Inject
    public SqlServerLock(ObjectMapper om, DataSource dataSource, SqlServerConfiguration configuration) throws UnknownHostException{
        super(om, dataSource);
        LOCK_NAMESPACE = configuration.getProperty("workflow.decider.locking.namespace", "GLBL");
        long cleanInterval = configuration.getLockCleanInterval();
        String host = "", rack = configuration.getProperty("LOCAL_RACK", ""), port = configuration.getProperty("conductor.jetty.server.port", "8080");
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = InetAddress.getLocalHost().getHostAddress();
        }
        finally {
            host += ":"+port;
        }
        if (rack.isEmpty()) {
            INSTANCE_IDENTIFIER = host;
        } else {
            INSTANCE_IDENTIFIER = String.format("%s%c%s", host, ':', rack);
        }
        if(cleanInterval > 0)
            Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(this::deleteExpired,
                        cleanInterval, cleanInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void acquireLock(String lockId) {
        acquireLock(lockId, 0, TimeUnit.SECONDS);
    }
    
    @Override
    public boolean acquireLock(String lockId, long timeToTry, TimeUnit unit) {
        throw new UnsupportedOperationException("This is very dangerous. Always lock with leaseTime");
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit) {
        return this.tryAcquire(
            lockId,
            this.getHolderId(),
            TimeUnit.MILLISECONDS.convert(timeToTry, unit),
            TimeUnit.MILLISECONDS.convert(leaseTime, unit)
        );
    }

    @Override
    public void releaseLock(String lockId) {
        deleteLock(lockId);
    }

    @Override
    public void deleteLock(String lockId) {
        // Preform a logical delete
        final String SQL = String.join("\n", 
            "UPDATE [data].[reentrant_lock] WITH(RowLock,xLock)",
            "SET expire_time=SYSDATETIME()",
            "WHERE ns=? AND lock_id = ?;"
        );
        getWithRetriedTransactions(tx -> query(tx, SQL, q -> q.addParameter(LOCK_NAMESPACE).addParameter(lockId).executeDelete()));
    }

    private boolean tryAcquire(String lockId, String holderId, long timeToTryMillis, long leaseTimeMillis) {
        final String SQL = String.join("\n", 
            "MERGE [data].[reentrant_lock] WITH(RowLock,xLock) target",
            "USING (SELECT ? as col1, CONVERT(UNIQUEIDENTIFIER, ?) AS col2, ? AS col3, DATEADD(millisecond, ?, SYSDATETIME()) AS col4) AS source(ns, lock_id, holder_id, expire_time)",
            "ON source.ns = target.ns AND source.lock_id = target.lock_id AND (source.holder_id = target.holder_id OR target.expire_time < SYSDATETIME())",
            "WHEN MATCHED THEN",
            "UPDATE SET target.expire_time=source.expire_time,target.holder_id=source.holder_id",
            "WHEN NOT MATCHED THEN",
            "INSERT (ns, lock_id, holder_id, expire_time)",
            "VALUES (source.ns, source.lock_id, source.holder_id, source.expire_time);"
        );
        long start = System.currentTimeMillis();

        try {
            return getWithTransactionWithOutErrorPropagation(
                tx -> {
                    int rows = 0;
                    do {
                        rows = query(tx, SQL, q -> q.addParameter(LOCK_NAMESPACE).addParameter(lockId).addParameter(holderId).addParameter(leaseTimeMillis).executeUpdate());
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    } while (rows < 1 && ((System.currentTimeMillis() - start) < timeToTryMillis));
                    if (rows > 1) {
                        logger.error("ERROR!!!!!!!!!");
                    }
                    return rows > 0;
            });    
        } catch (Exception e) {
            // When the query fails we assume false
            return false;
        }
    }

    private void deleteExpired() {
        final String SQL = String.join("\n", 
            "DELETE FROM [data].[reentrant_lock] WITH(xLock)",
            "WHERE ns=? AND expire_time < SYSDATETIME();"
        );
        int deleted = getWithTransactionWithOutErrorPropagation(tx -> 
            query(tx, SQL, q -> q.addParameter(LOCK_NAMESPACE).executeUpdate()));
        logger.info("Deleted "+deleted+" expired locks");
    }

    public String getHolderId()
    {
        return String.format("%s%c%d", INSTANCE_IDENTIFIER, '-', Thread.currentThread().getId());
    }
}

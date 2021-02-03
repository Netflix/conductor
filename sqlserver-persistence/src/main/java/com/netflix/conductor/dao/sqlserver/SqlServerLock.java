package com.netflix.conductor.dao.sqlserver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.netflix.conductor.core.utils.Lock;
import com.netflix.conductor.sqlserver.SqlServerConfiguration;

public class SqlServerLock extends SqlServerBaseDAO implements Lock {

    private static String LOCK_NAMESPACE = "";
    private static String INSTANCE_IDENTIFIER = "";

    @Inject
    public SqlServerLock(ObjectMapper om, DataSource dataSource, SqlServerConfiguration configuration) throws UnknownHostException{
        super(om, dataSource);
        LOCK_NAMESPACE = configuration.getProperty("workflow.decider.locking.namespace", "");
        
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
        withTransaction(tx -> deleteExpired(tx, lockId));
        
        return this.tryAcquire(
            this.getLockId(lockId),
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
        final String SQL = String.join("\n", 
            "DELETE FROM [dbo].[reentrant_lock]",
            "WHERE lock_id = ?"
        );
        withTransaction(tx -> execute(tx, SQL, q -> q.addParameter(lockId).executeDelete()));
    }

    private boolean tryAcquire(String lockId, String holderId, long timeToTryMillis, long leaseTimeMillis) {
        final String SQL = String.join("\n", 
            "MERGE [dbo].[reentrant_lock] as target",
            "USING (SELECT ? AS col1, ? AS col2, DATEADD(millisecond, ?, SYSDATETIMEOFFSET()) AS col3) AS source(lock_id, holder_id, expire_time)",
            "ON source.lock_id = target.lock_id AND source.holder_id = target.holder_id",
            "WHEN MATCHED THEN",
            "UPDATE SET target.expire_time=source.expire_time",
            "WHEN NOT MATCHED THEN",
            "INSERT (lock_id, holder_id, expire_time)",
            "VALUES (source.lock_id, source.holder_id, source.expire_time);"
        );
        long start = System.currentTimeMillis();
        
        return getWithTransactionWithOutErrorPropagation(tx -> {
            int rows = 0;
            do {
                rows = query(tx, SQL, q -> q.addParameter(lockId).addParameter(holderId).addParameter(leaseTimeMillis).executeUpdate());
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                deleteExpired(tx, lockId);
            } while (rows < 1 && ((System.currentTimeMillis() - start) < timeToTryMillis));
            return rows > 0;
        });
    }

    private void deleteExpired(Connection conn, String lockId) {
        final String SQL = String.join("\n", 
            "DELETE FROM [dbo].[reentrant_lock]",
            "WHERE lock_id = ? AND expire_time < SYSDATETIMEOFFSET()"
        );
        execute(conn, SQL, q -> q.addParameter(lockId).executeDelete());
    }

    private String getHolderId()
    {
        return String.format("%s%c%d", INSTANCE_IDENTIFIER, '-', Thread.currentThread().getId());
    }

    private String getLockId(String lockId)
    {
        if (lockId.isEmpty()) {
            return lockId;
        }
        return String.format("%s%c%s", LOCK_NAMESPACE, '.', lockId);
    }
}

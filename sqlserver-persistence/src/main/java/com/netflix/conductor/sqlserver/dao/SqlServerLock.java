package com.netflix.conductor.sqlserver.dao;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.grpc.server.GRPCServerProperties;
import com.netflix.conductor.sqlserver.config.SqlServerProperties;

public class SqlServerLock extends SqlServerBaseDAO implements Lock {

    private static String LOCK_NAMESPACE = "";
    private static String INSTANCE_IDENTIFIER = "";

    public SqlServerLock(ObjectMapper om, DataSource dataSource, 
        SqlServerProperties properties, GRPCServerProperties grpc) throws UnknownHostException{
        super(om, dataSource);
        LOCK_NAMESPACE = properties.getLockNamespace();
        
        String host = "", zone = properties.getZone(), port = Integer.toString(grpc.getPort());
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = InetAddress.getLocalHost().getHostAddress();
        }
        finally {
            host += ":"+port;
        }
        if (zone.isEmpty()) {
            INSTANCE_IDENTIFIER = host;
        } else {
            INSTANCE_IDENTIFIER = String.format("%s%c%s", host, ':', zone);
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
            "DELETE FROM [data].[reentrant_lock] WITH(TabLock,xLock)",
            "WHERE lock_id = ?;"
        );
        getWithRetriedTransactions(tx -> query(tx, SQL, q -> q.addParameter(lockId).executeDelete()));
    }

    private boolean tryAcquire(String lockId, String holderId, long timeToTryMillis, long leaseTimeMillis) {
        final String SQL = String.join("\n", 
            "MERGE [data].[reentrant_lock] WITH(TabLock,xLock) target",
            "USING (SELECT ? AS col1, ? AS col2, DATEADD(millisecond, ?, SYSDATETIMEOFFSET()) AS col3) AS source(lock_id, holder_id, expire_time)",
            "ON source.lock_id = target.lock_id AND source.holder_id = target.holder_id",
            "WHEN MATCHED THEN",
            "UPDATE SET target.expire_time=source.expire_time",
            "WHEN NOT MATCHED THEN",
            "INSERT (lock_id, holder_id, expire_time)",
            "VALUES (source.lock_id, source.holder_id, source.expire_time);"
        );
        long start = System.currentTimeMillis();

        try {
            return getWithTransactionWithOutErrorPropagation(
                tx -> {
                    int rows = 0;
                    do {
                        rows = query(tx, SQL, q -> q.addParameter(lockId).addParameter(holderId).addParameter(leaseTimeMillis).executeUpdate());
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                        deleteExpired(tx, lockId);
                    } while (rows < 1 && ((System.currentTimeMillis() - start) < timeToTryMillis));
                    if (rows > 1) {
                        System.out.println("ERROR!!!!!!!!!");
                    }
                    return rows > 0;
            });    
        } catch (Exception e) {
            // When the query fails we assume false
            return false;
        }
    }

    private void deleteExpired(Connection conn, String lockId) {
        final String SQL = String.join("\n", 
            "DELETE FROM [data].[reentrant_lock] WITH(xLock)",
            "WHERE lock_id = ? AND expire_time < SYSDATETIMEOFFSET();"
        );
        execute(conn, SQL, q -> q.addParameter(lockId).executeUpdate());
    }

    public String getHolderId()
    {
        return String.format("%s%c%d", INSTANCE_IDENTIFIER, '-', Thread.currentThread().getId());
    }

    public String getLockId(String lockId)
    {
        if (LOCK_NAMESPACE.isEmpty()) {
            return lockId;
        }
        return String.format("%s%c%s", LOCK_NAMESPACE, '.', lockId);
    }
}

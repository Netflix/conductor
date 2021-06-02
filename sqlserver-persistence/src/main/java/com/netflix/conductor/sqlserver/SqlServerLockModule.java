package com.netflix.conductor.sqlserver;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.netflix.conductor.core.utils.Lock;
import com.netflix.conductor.dao.sqlserver.SqlServerLock;

public class SqlServerLockModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Lock.class).to(SqlServerLock.class).in(Singleton.class);
    }
    
}

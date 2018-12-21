package com.netflix.conductor.contribs;

import com.google.inject.AbstractModule;
import com.netflix.conductor.contribs.dynomite.*;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.queues.ShardSupplier;

public class StaticDynomiteClusterModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(HostTokenSupplier.class).toProvider(ConfigurationHostTokenSupplierProvider.class);
        bind(HostSupplier.class).toProvider(ConfigurationHostSupplierProvider.class);
        bind(TokenMapSupplier.class).toProvider(TokenMapSupplierProvider.class);
        bind(ShardSupplier.class).toProvider(StaticDynoShardSupplierProvider.class);
    }
}

package com.netflix.conductor.contribs;

import com.google.inject.AbstractModule;
import com.netflix.conductor.contribs.dynomite.ConfigurationHostSupplierProvider;
import com.netflix.conductor.contribs.dynomite.ConfigurationHostTokenSupplierProvider;
import com.netflix.conductor.contribs.dynomite.HostTokenSupplier;
import com.netflix.conductor.contribs.dynomite.TokenMapSupplierProvider;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;

public class DynomiteClusterModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(HostTokenSupplier.class).toProvider(ConfigurationHostTokenSupplierProvider.class);
        bind(HostSupplier.class).toProvider(ConfigurationHostSupplierProvider.class);
        bind(TokenMapSupplier.class).toProvider(TokenMapSupplierProvider.class);
    }
}

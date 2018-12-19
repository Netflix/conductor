package com.netflix.conductor.contribs.dynomite;

import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.stream.Collectors;

public class ConfigurationHostSupplierProvider implements Provider<HostSupplier> {
    private final HostTokenSupplier hostTokenSupplier;

    @Inject
    public ConfigurationHostSupplierProvider(HostTokenSupplier hostTokenSupplier) {
        this.hostTokenSupplier = hostTokenSupplier;
    }

    @Override
    public HostSupplier get() {
        return () -> hostTokenSupplier
                .getHostsTokens()
                .stream()
                .map(HostToken::getHost)
                .collect(Collectors.toList());
    }
}

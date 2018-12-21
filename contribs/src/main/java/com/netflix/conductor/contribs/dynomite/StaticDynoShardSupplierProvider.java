package com.netflix.conductor.contribs.dynomite;

import com.google.inject.ProvisionException;
import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.shard.DynoShardSupplier;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.stream.Collectors;

public class StaticDynoShardSupplierProvider implements Provider<ShardSupplier> {

    private final HostSupplier hostSupplier;
    private final DynomiteConfiguration configuration;

    @Inject
    public StaticDynoShardSupplierProvider(HostSupplier hostSupplier, DynomiteConfiguration dynomiteConfiguration) {
        this.hostSupplier = hostSupplier;
        this.configuration = dynomiteConfiguration;
    }

    @Override
    public ShardSupplier get() {
        if(configuration.getAvailabilityZone() == null) {
            throw new ProvisionException(
                    "Availability zone is not defined.  Ensure Configuration.getAvailabilityZone() returns a non-null " +
                            "and non-empty value."
            );
        }

        String localDC = configuration.getAvailabilityZone().replaceAll(configuration.getRegion(), "");

        return new DynoShardSupplier(this::getHostsFromAffinityRack, configuration.getRegion(), localDC);
    }

    private List<Host> getHostsFromAffinityRack() {
        return hostSupplier.getHosts()
                .stream()
                .filter(host -> host.getRack().equals(configuration.getAvailabilityZone()))
                .collect(Collectors.toList());
    }
}

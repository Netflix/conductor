package com.netflix.conductor.contribs.dynomite;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.conductor.dyno.DynomiteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigurationHostTokenSupplierProvider implements Provider<HostTokenSupplier> {
    private static final String DELIMITER = ";";
    private static Logger logger = LoggerFactory.getLogger(ConfigurationHostTokenSupplierProvider.class);

    private final DynomiteConfiguration configuration;

    @Inject
    public ConfigurationHostTokenSupplierProvider(DynomiteConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public HostTokenSupplier get() {
        return () -> parseHostsFromConfig(configuration);
    }

    private List<HostToken> parseHostsFromConfig(DynomiteConfiguration configuration) {
        String hosts = configuration.getHosts();
        if(hosts == null) {
            String message = String.format(
                    "Missing dynomite/redis hosts.  Ensure '%s' has been set in the supplied configuration.",
                    DynomiteConfiguration.HOSTS_PROPERTY_NAME
            );
            logger.error(message);
            throw new RuntimeException(message);
        }
        return parseHostsFrom(hosts);
    }

    private List<HostToken> parseHostsFrom(String hostConfig){
        List<String> hostConfigs = Arrays.asList(hostConfig.split(DELIMITER));

        return hostConfigs.stream()
                .map(this::buildHostToken)
                .collect(Collectors.toList());
    }

    private HostToken buildHostToken(String hc) {
        String[] hostConfigValues = hc.split(":");
        String host = hostConfigValues[0];
        int port = Integer.parseInt(hostConfigValues[1]);
        String rack = hostConfigValues[2];
        String datacenter = hostConfigValues[3];
        Long token = Long.valueOf(hostConfigValues[4]);
        return new HostToken(token, new Host(host, null, port, rack, datacenter, Host.Status.Up));
    }
}

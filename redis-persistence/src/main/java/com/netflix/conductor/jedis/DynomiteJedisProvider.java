/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Provider;

import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.commands.JedisCommands;

public class DynomiteJedisProvider implements Provider<JedisCommands> {

        private static Logger logger = LoggerFactory.getLogger(DynomiteJedisProvider.class);

        private final HostSupplier hostSupplier;
        private final TokenMapSupplier tokenMapSupplier;
        private final DynomiteConfiguration configuration;
        private CustomGeminiClusterConfig geminiConfig;
        private final boolean isClusterConfig;

        @Inject
        public DynomiteJedisProvider(DynomiteConfiguration configuration, HostSupplier hostSupplier,
                        TokenMapSupplier tokenMapSupplier) {
                this.configuration = configuration;
                this.hostSupplier = hostSupplier;

                // if multiple hosts, then use custom Gemini cluster config
                if (isClusterConfig = hasMultipleHosts(configuration)) {
                        this.geminiConfig = new CustomGeminiClusterConfig();
                        this.tokenMapSupplier = geminiConfig.getTokenMapSupplier();
                } else {
                        this.tokenMapSupplier = tokenMapSupplier;
                }
    }

    @Override
    public JedisCommands get() {
        ConnectionPoolConfigurationImpl connectionPoolConfiguration = isClusterConfig ? geminiConfig.getConnectionPoolConfigurationImpl() :
                new ConnectionPoolConfigurationImpl(configuration.getClusterName())
                .withTokenSupplier(tokenMapSupplier)
                .setLocalRack(configuration.getAvailabilityZone())
                .setLocalDataCenter(configuration.getRegion())
                .setSocketTimeout(0)
                .setConnectTimeout(0)
                .setMaxConnsPerHost(
                        configuration.getMaxConnectionsPerHost()
                );


        return new DynoJedisClient.Builder()
                .withHostSupplier(hostSupplier)
                .withApplicationName(configuration.getAppId())
                .withDynomiteClusterName(configuration.getClusterName())
                .withCPConfig(connectionPoolConfiguration)
                .build();
    }


    private boolean hasMultipleHosts( DynomiteConfiguration dynConfiguration){
        boolean isMultipleHosts = false;

        if (dynConfiguration == null || dynConfiguration.getHosts() == null) {
                return isMultipleHosts;
        }

        // split dynamo cluster hosts string and check if we have more than 1
        // format is host:port:rack separated by semicolon
        isMultipleHosts = dynConfiguration.getHosts().split(";").length > 1;

        return isMultipleHosts;
    }

    // Inner class which deals with custom Gemini configs
        private class CustomGeminiClusterConfig {

                private TokenMapSupplier getTokenMapSupplier() {

                        // obtain list of hosts from hostSupplier and populate tokenMap
                        List<Host> hostsList = hostSupplier.getHosts();

                        Map<Host, HostToken> tokenMap = Objects.isNull(hostsList) ? new HashMap<>(1)
                                        : hostsList.stream()
                                                .collect(Collectors.toMap(host -> host, host -> new HostToken(4294967295L, host)));

                        return new TokenMapSupplier() {
                                @Override
                                public List<HostToken> getTokens(Set<Host> activeHosts) {
                                        return new ArrayList<HostToken>(tokenMap.values());
                                }

                                @Override
                                public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {

                                        HostToken hostToken = tokenMap.entrySet().stream()
                                                        .filter(entry -> entry.getKey().getHostName().equals(host.getHostName()))
                                                        .map(Map.Entry::getValue).findAny().orElse(null);

                                        return hostToken;

                                }
                        };
                }

                private ConnectionPoolConfigurationImpl getConnectionPoolConfigurationImpl() {

                        logger.info("Starting conductor server using dynomite/redis cluster "
                                        + configuration.getClusterName());

                        return new ConnectionPoolConfigurationImpl(configuration.getClusterName())
                                        .setLoadBalancingStrategy(
                                                        ConnectionPoolConfiguration.LoadBalancingStrategy.RoundRobin)
                                        .withTokenSupplier(tokenMapSupplier)
                                        .setLocalRack(configuration.getAvailabilityZone())
                                        .setLocalDataCenter(configuration.getRegion())
                                        .setMaxConnsPerHost(configuration.getMaxConnectionsPerHost());
                }

        }


}

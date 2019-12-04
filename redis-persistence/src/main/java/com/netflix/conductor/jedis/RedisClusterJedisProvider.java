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

import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.commands.JedisCommands;

public class RedisClusterJedisProvider implements Provider<JedisCommands> {

    private final HostSupplier hostSupplier;
    private final DynomiteConfiguration configuration;

    @Inject
    public RedisClusterJedisProvider(HostSupplier hostSupplier, DynomiteConfiguration configuration){
        this.hostSupplier = hostSupplier;
        this.configuration = configuration;
    }

    @Override
    public JedisCommands get() {

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMinIdle(configuration.getMinIdleConnectionsPerHost());
        poolConfig.setMaxTotal(configuration.getMaxConnectionsPerHost());

        Host host = new ArrayList<>(hostSupplier.getHosts()).get(0);

        //Jedis Cluster will attempt to discover cluster nodes automatically
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort(host.getHostName(), host.getPort()));

        redis.clients.jedis.JedisCluster clusterClient = new redis.clients.jedis.JedisCluster(jedisClusterNodes, poolConfig);

        return new JedisCluster(clusterClient);
    }
}

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
package com.netflix.conductor.server;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.netflix.conductor.dyno.DynoShardSupplierProvider;
import com.netflix.conductor.dyno.DynomiteConfiguration;
import com.netflix.conductor.dyno.RedisQueuesProvider;
import com.netflix.conductor.dyno.SystemPropertiesDynomiteConfiguration;
import com.netflix.conductor.jedis.ConfigurationHostSupplierProvider;
import com.netflix.conductor.jedis.RedisSentinelJedisProvider;
import com.netflix.conductor.jedis.TokenMapSupplierProvider;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.queues.ShardSupplier;
import redis.clients.jedis.commands.JedisCommands;

public class RedisSentinelModule extends AbstractModule {
    @Override
    protected void configure(){
        bind(DynomiteConfiguration.class).to(SystemPropertiesDynomiteConfiguration.class);
        bind(JedisCommands.class).toProvider(RedisSentinelJedisProvider.class).asEagerSingleton();
        bind(JedisCommands.class)
                .annotatedWith(Names.named(RedisQueuesProvider.READ_CLIENT_INJECTION_NAME))
                .toProvider(RedisSentinelJedisProvider.class)
                .asEagerSingleton();
        bind(HostSupplier.class).toProvider(ConfigurationHostSupplierProvider.class);
        bind(TokenMapSupplier.class).toProvider(TokenMapSupplierProvider.class);
        bind(ShardSupplier.class).toProvider(DynoShardSupplierProvider.class);
    }
}

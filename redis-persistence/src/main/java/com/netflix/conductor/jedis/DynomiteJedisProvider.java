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
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.jedis.DynoJedisClient;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import redis.clients.jedis.commands.JedisCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynomiteJedisProvider implements Provider<JedisCommands> {

    private final HostSupplier hostSupplier;
    private final TokenMapSupplier tokenMapSupplier;
    private final DynomiteConfiguration configuration;

    private static Logger logger = LoggerFactory.getLogger(DynomiteJedisProvider.class);

    @Inject
    public DynomiteJedisProvider(
            DynomiteConfiguration configuration,
            HostSupplier hostSupplier,
            TokenMapSupplier tokenMapSupplier
    ){
        this.configuration = configuration;
        this.hostSupplier = hostSupplier;
        this.tokenMapSupplier = tokenMapSupplier;
    }

    private SSLContext createAndInitSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, null, null);

        return sslContext;
    }

    @Override
    public JedisCommands get() {
        ConnectionPoolConfigurationImpl connectionPoolConfiguration =
                new ConnectionPoolConfigurationImpl(configuration.getClusterName())
                .withTokenSupplier(tokenMapSupplier)
                .setLocalRack(configuration.getAvailabilityZone())
                .setLocalDataCenter(configuration.getRegion())
                .setSocketTimeout(0)
                .setConnectTimeout(0)
                .setMaxConnsPerHost(
                        configuration.getMaxConnectionsPerHost()
                );

        final DynoJedisClient.Builder builder = new DynoJedisClient.Builder()
                .withHostSupplier(hostSupplier)
                .withApplicationName(configuration.getAppId())
                .withDynomiteClusterName(configuration.getClusterName())
                .withCPConfig(connectionPoolConfiguration);

        if (this.configuration.shouldUseSSL()) {
                try {
                        final SSLContext sslContext = createAndInitSSLContext();
                        builder.withSSLSocketFactory(sslContext.getSocketFactory());

                        logger.info("Functioning in SSL mode.");
                } catch (NoSuchAlgorithmException | KeyManagementException exc) {
                        logger.warn("Could not initialise SSLContext, falling back to non-ssl connection.");
                }
        }

        return builder.build();
    }
}

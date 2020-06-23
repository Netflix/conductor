package com.netflix.conductor.health;

import com.google.inject.AbstractModule;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.runtime.health.api.HealthCheckAggregator;
import com.netflix.runtime.health.api.HealthCheckStatus;
import com.netflix.runtime.health.guice.HealthModule;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisException;

import java.util.concurrent.ExecutionException;

public class RedisHealthModuleTest {

    @Test
    public void testNoIndicators() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new HealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertTrue(healthCheckStatus.isHealthy());
        Assert.assertEquals(0, healthCheckStatus.getHealthResults().size());
    }


    @Test
    public void testHealthyKeyExistsIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                bind(JedisCommands.class).toInstance(new Jedis() {
                    @Override public Boolean exists(final String key) {
                        return true;
                    }
                });
            }
        }, new RedisHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertTrue(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

    @Test
    public void testHealthyKeyNotExistsIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                bind(JedisCommands.class).toInstance(new Jedis() {
                    @Override public Boolean exists(final String key) {
                        return false;
                    }
                });
            }
        }, new RedisHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertTrue(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

    @Test
    public void testUnHealthyExceptionIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                bind(JedisCommands.class).toInstance(new Jedis() {
                   @Override public Boolean exists(final String key) {
                       throw new JedisException(new Exception());
                   }
                });
            }
        }, new RedisHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertFalse(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

}

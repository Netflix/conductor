package com.netflix.conductor.health;

import com.google.inject.AbstractModule;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.runtime.health.api.HealthCheckAggregator;
import com.netflix.runtime.health.api.HealthCheckStatus;
import com.netflix.runtime.health.guice.HealthModule;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//import com.netflix.conductor.dao.elasticsearch.ClientMock;

public class ElasticSearchHealthModuleTest {

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
    public void testHealthyStatusGreenIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                ClusterHealthResponse responseMock = mock(ClusterHealthResponse.class, Mockito.RETURNS_DEEP_STUBS);
                when(responseMock.isTimedOut()).thenReturn(false);
                when(responseMock.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
                Client esClientMock = mock(Client.class, Mockito.RETURNS_DEEP_STUBS);
                when(esClientMock.admin().cluster().prepareHealth().get()).thenReturn(responseMock);
                bind(Client.class).toInstance(esClientMock);
            }
        }, new ElasticSearchHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertTrue(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

    @Test
    public void testHealthyStatusYellowIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                ClusterHealthResponse responseMock = mock(ClusterHealthResponse.class, Mockito.RETURNS_DEEP_STUBS);
                when(responseMock.isTimedOut()).thenReturn(false);
                when(responseMock.getStatus()).thenReturn(ClusterHealthStatus.YELLOW);
                Client esClientMock = mock(Client.class, Mockito.RETURNS_DEEP_STUBS);
                when(esClientMock.admin().cluster().prepareHealth().get()).thenReturn(responseMock);
                bind(Client.class).toInstance(esClientMock);
            }
        }, new ElasticSearchHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertTrue(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

    @Test
    public void testUnHealthyStatusRedIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                ClusterHealthResponse responseMock = mock(ClusterHealthResponse.class, Mockito.RETURNS_DEEP_STUBS);
                when(responseMock.isTimedOut()).thenReturn(false);
                when(responseMock.getStatus()).thenReturn(ClusterHealthStatus.RED);
                Client esClientMock = mock(Client.class, Mockito.RETURNS_DEEP_STUBS);
                when(esClientMock.admin().cluster().prepareHealth().get()).thenReturn(responseMock);
                bind(Client.class).toInstance(esClientMock);
            }
        }, new ElasticSearchHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertFalse(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

    @Test
    public void testUnHealthyTimeoutIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                ClusterHealthResponse responseMock = mock(ClusterHealthResponse.class, Mockito.RETURNS_DEEP_STUBS);
                when(responseMock.isTimedOut()).thenReturn(true);
                when(responseMock.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
                Client esClientMock = mock(Client.class, Mockito.RETURNS_DEEP_STUBS);
                when(esClientMock.admin().cluster().prepareHealth().get()).thenReturn(responseMock);
                bind(Client.class).toInstance(esClientMock);
            }
        }, new ElasticSearchHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertFalse(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }

    @Test
    public void testUnHealthyExceptionIndicator() throws InterruptedException, ExecutionException {
        LifecycleInjector injector = InjectorBuilder.fromModules(new AbstractModule() {
            @Override
            public void configure() {
                Client esClientMock = mock(Client.class, Mockito.RETURNS_DEEP_STUBS);
                when(esClientMock.admin().cluster().prepareHealth().get()).thenThrow(new RuntimeException());
                bind(Client.class).toInstance(esClientMock);
            }
        }, new ElasticSearchHealthModule(), new ArchaiusModule()).createInjector();
        HealthCheckAggregator aggregator = injector.getInstance(HealthCheckAggregator.class);

        Assert.assertNotNull(aggregator);
        HealthCheckStatus healthCheckStatus = aggregator.check().get();
        Assert.assertFalse(healthCheckStatus.isHealthy());
        Assert.assertEquals(1, healthCheckStatus.getHealthResults().size());
    }
}

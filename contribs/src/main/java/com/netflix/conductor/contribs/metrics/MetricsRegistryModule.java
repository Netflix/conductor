package com.netflix.conductor.contribs.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.netflix.spectator.api.Clock;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.metrics3.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton metrics registry.
 */
public class MetricsRegistryModule extends AbstractModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsRegistryModule.class);

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    public static final MetricsRegistry METRICS_REGISTRY = new MetricsRegistry(Clock.SYSTEM, METRIC_REGISTRY);
    static {
        Spectator.globalRegistry().add(METRICS_REGISTRY);
    }

    @Override
    protected void configure() {
        LOGGER.info("Metrics registry module initialized");
        bind(MetricRegistry.class).toInstance(METRIC_REGISTRY);
        bind(MetricsRegistry.class).toInstance(METRICS_REGISTRY);
    }
}

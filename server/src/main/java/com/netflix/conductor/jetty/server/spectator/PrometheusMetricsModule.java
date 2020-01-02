package com.netflix.conductor.jetty.server.spectator;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.netflix.conductor.jetty.server.spectator.prometheus.SpectatorExports;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Spectator;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;

@Singleton
public class PrometheusMetricsModule extends ServletModule
{
    @Override
    protected void configureServlets() {
        Spectator.globalRegistry().add(new DefaultRegistry());
        Collector collector = new SpectatorExports(Spectator.globalRegistry());
        CollectorRegistry.defaultRegistry.register(collector);
        serve("/metrics").with(new MetricsServlet());
    }
}

package com.netflix.conductor.jetty.server.spectator.prometheus;

import com.netflix.spectator.api.CompositeRegistry;
import com.netflix.spectator.api.Measurement;
import com.netflix.spectator.api.Meter;
import com.netflix.spectator.api.Tag;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Collect metrics from spectator CompositeRegistry,such as Spectator.globalRegistry()
 *
 * Taken from https://github.com/prometheus/client_java/pull/370
 */
public class SpectatorExports extends io.prometheus.client.Collector implements
        io.prometheus.client.Collector.Describable {

    private final CompositeRegistry registry;

    public SpectatorExports(CompositeRegistry registry) {
        this.registry = registry;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> familySamples = new ArrayList<MetricFamilySamples>();
        if (registry == null) {
            return familySamples;
        }

        List<Sample> samples = new ArrayList<Sample>();
        Iterator<Meter> meters = registry.iterator();
        while (meters.hasNext()) {
            Meter meter = meters.next();
            if (meter != null) {
                Iterator<Measurement> measurements = meter.measure().iterator();
                while (measurements.hasNext()) {
                    samples.add(convertMeasurementToSample(measurements.next()));
                }
            }
        }

        familySamples.add(
                new MetricFamilySamples("SpectatorMetrics", Type.UNTYPED, "Generated from Spectator CompositeRegistry",
                        samples));

        return familySamples;
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return new ArrayList<MetricFamilySamples>();
    }

    private Sample convertMeasurementToSample(Measurement measurement) {
        String prometheusName = sanitizeMetricName(measurement.id().name());
        List<String> labelNames = new ArrayList<String>();
        List<String> labelValues = new ArrayList<String>();
        for (Tag tag : measurement.id().tags()) {
            labelNames.add(tag.key());
            labelValues.add(tag.value());
        }
        return new Sample(prometheusName, labelNames, labelValues, measurement.value());
    }

    private static final Pattern METRIC_NAME_RE = Pattern.compile("[^a-zA-Z0-9:_]");

    /**
     * Replace all unsupported chars with '_', prepend '_' if name starts with digit.
     *
     * @param originalName
     *            original metric name.
     * @return the sanitized metric name.
     */
    public static String sanitizeMetricName(String originalName) {
        String name = METRIC_NAME_RE.matcher(originalName).replaceAll("_");
        if (!name.isEmpty() && Character.isDigit(name.charAt(0))) {
            name = "_" + name;
        }
        return name;
    }
}

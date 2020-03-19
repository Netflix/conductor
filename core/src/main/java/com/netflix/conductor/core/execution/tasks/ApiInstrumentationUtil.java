package com.netflix.conductor.core.execution.tasks;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class ApiInstrumentationUtil {

    private ApiInstrumentationUtil() {
    }

    public enum ApiResponse {SUCCESS, FAILURE}

    public static final Counter requestTotal = Counter.build()
            .name("request_total")
            .labelNames("api", "response", "exception", "code")
            .help("Request Total").register();
    public static final Histogram requestLatency = Histogram.build()
            .name("request_latency")
            .labelNames("api")
            .help("Request Latency").register();
    public static final Gauge requestInProgress = Gauge.build()
            .name("request_in_progress")
            .labelNames("api")
            .help("In progress request count").register();
}
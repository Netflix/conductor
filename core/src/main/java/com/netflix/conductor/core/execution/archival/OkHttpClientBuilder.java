package com.netflix.conductor.core.execution.archival;

import com.google.inject.Singleton;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.util.concurrent.TimeUnit;

@Singleton
public class OkHttpClientBuilder {

    private final int DEFAULT_KEEP_ALIVE_DURATION = 5;

    public OkHttpClientBuilder() {
    }

    public OkHttpClient buildClient(ApiProperties apiProperties) {
        ConnectionPool connectionPool = new ConnectionPool(apiProperties.getPoolSize(), DEFAULT_KEEP_ALIVE_DURATION, TimeUnit.MINUTES);
        return new OkHttpClient.Builder()
                .addNetworkInterceptor(chain -> {
                    Request request = chain
                            .request()
                            .newBuilder()
                            .addHeader("Content-Type", "application/json")
                            .build();
                    return chain.proceed(request);
                })
                .connectionPool(connectionPool)
                .connectTimeout(apiProperties.getConnectTimeout(), TimeUnit.MILLISECONDS)
                .readTimeout(apiProperties.getReadTimeout(), TimeUnit.MILLISECONDS)
                .build();
    }
}
package com.netflix.conductor.core.execution.archival;

public class ApiProperties {

    private static final int DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS = 3000;
    private static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 3000;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_POOL_SIZE = 50;
    private static final int DEFAULT_POOL_DURATION = 5;

    private String host;

    private  String url;

    private String username;

    private String password;

    private int readTimeout = DEFAULT_READ_TIMEOUT_IN_MILLIS;

    private int connectTimeout = DEFAULT_CONNECTION_TIMEOUT_IN_MILLIS;

    private int retryCount = DEFAULT_MAX_RETRIES;

    private int poolSize = DEFAULT_POOL_SIZE;

    private int poolDuration = DEFAULT_POOL_DURATION;

    public ApiProperties(String host, int readTimeout, int connectTimeout) {
        this.host = host;
        this.readTimeout = readTimeout;
        this.connectTimeout = connectTimeout;
    }

    public String getHost() {
        return this.host;
    }

    public int getPoolSize() {
        return this.poolSize;
    }

    public long getConnectTimeout() {
        return this.connectTimeout;
    }

    public long getReadTimeout() {
        return this.readTimeout;
    }
}


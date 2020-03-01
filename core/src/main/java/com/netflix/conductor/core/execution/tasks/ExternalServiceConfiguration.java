package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.core.config.Configuration;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ExternalServiceConfiguration {

    private Configuration configuration;

    @Inject
    private final OkHttpClientBuilder okHttpClientBuilder;

    @Inject
    public ExternalServiceConfiguration(Configuration configuration, OkHttpClientBuilder okHttpClientBuilder) {
        this.configuration = configuration;
        this.okHttpClientBuilder = okHttpClientBuilder;
    }

    @Singleton
    public ApiProperties getFloEndpoint() {
        return new ApiProperties(configuration.getProperty("flo.host", null),
                Integer.parseInt(configuration.getProperty("flo.connect.timeout", null)),
                Integer.parseInt(configuration.getProperty("flo.read.timeout", null)));
    }

    @Singleton
    public Retrofit getFloRetrofitBean(ApiProperties apiProperties) {
        OkHttpClient httpClient = okHttpClientBuilder.buildClient(apiProperties);
        return ReminderRetrofitUtil.getDefaultRetrofitObject(httpClient, apiProperties.getHost());
    }

}

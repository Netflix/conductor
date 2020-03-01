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
    private OkHttpClientBuilder okHttpClientBuilder;

    @Inject
    public ExternalServiceConfiguration(Configuration configuration, OkHttpClientBuilder okHttpClientBuilder) {
        this.configuration = configuration;
        this.okHttpClientBuilder = okHttpClientBuilder;
    }

    @Singleton
    public ApiProperties getReminderEndpoint() {
        return new ApiProperties(configuration.getProperty("reminder.host", "http://localhost:8080"),
                Integer.parseInt(configuration.getProperty("reminder.connect.timeout", "300")),
                Integer.parseInt(configuration.getProperty("reminder.read.timeout", "300")));
    }

    @Singleton
    public Retrofit getReminderRetrofitBean(ApiProperties apiProperties) {
        OkHttpClient httpClient = okHttpClientBuilder.buildClient(apiProperties);
        return ReminderRetrofitUtil.getDefaultRetrofitObject(httpClient, apiProperties.getHost());
    }

}

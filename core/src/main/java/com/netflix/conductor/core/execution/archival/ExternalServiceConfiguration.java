package com.netflix.conductor.core.execution.archival;

import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ExternalServiceConfiguration {

    private SystemPropertiesConfiguration configuration;

    private OkHttpClientBuilder okHttpClientBuilder;

    @Inject
    public ExternalServiceConfiguration(SystemPropertiesConfiguration systemPropertiesConfiguration, OkHttpClientBuilder okHttpClientBuilder) {
        this.configuration = systemPropertiesConfiguration;
        this.okHttpClientBuilder = okHttpClientBuilder;
    }

    @Singleton
    public ApiProperties getReminderEndpoint() {
        return new ApiProperties(configuration.getProperty("reminder.host", "http://reminder-service.swiggy.prod"),
                Integer.parseInt(configuration.getProperty("reminder.connect.timeout", "300")),
                Integer.parseInt(configuration.getProperty("reminder.read.timeout", "300")));
    }

    @Singleton
    public Retrofit getReminderRetrofitBean(ApiProperties apiProperties) {
        OkHttpClient httpClient = okHttpClientBuilder.buildClient(apiProperties);
        return ReminderRetrofitUtil.getDefaultRetrofitObject(httpClient, apiProperties.getHost());
    }

}

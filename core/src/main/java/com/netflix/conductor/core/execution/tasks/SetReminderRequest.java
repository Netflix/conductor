package com.netflix.conductor.core.execution.tasks;

import okhttp3.ResponseBody;
import retrofit2.Call;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SetReminderRequest {

    private final ExternalServiceConfiguration externalServiceConfiguration;
    private ReminderApi reminderApi;

    @Inject
    public SetReminderRequest(ExternalServiceConfiguration externalServiceConfiguration) {
        this.externalServiceConfiguration = externalServiceConfiguration;
        initialize();
    }

    private void initialize() {
        ApiProperties apiProperties = externalServiceConfiguration.getReminderEndpoint();
        this.reminderApi = externalServiceConfiguration.getReminderRetrofitBean(apiProperties).create(ReminderApi.class);
    }

    public RetrofitResponse<ResponseBody> setReminder(ReminderPayload request) {
        Call<ResponseBody> retrofitCall = reminderApi.setReminder(request);
        return ReminderRetrofitUtil.executeCall(retrofitCall, "setReminder");
    }
}

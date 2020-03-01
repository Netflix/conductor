package com.netflix.conductor.core.execution.tasks;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Retrofit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SetReminderRequest {

    private final Retrofit retrofit;
    private ReminderApi reminderApi;

    @Inject
    public SetReminderRequest(Retrofit retrofit) {
        this.retrofit = retrofit;
    }

    @PostConstruct
    private void initialize() {
        this.reminderApi = retrofit.create(ReminderApi.class);
    }

    public RetrofitResponse<ResponseBody> setReminder(ReminderPayload request) {
        Call<ResponseBody> retrofitCall = reminderApi.setReminder(request);
        return ReminderRetrofitUtil.executeCall(retrofitCall, "setReminder");
    }
}

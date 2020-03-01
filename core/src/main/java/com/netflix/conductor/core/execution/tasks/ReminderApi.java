package com.netflix.conductor.core.execution.tasks;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;

public interface ReminderApi {

    @Headers({
            "Accept: text/plain",
            "Content-Type: application/json"
    })
    @GET("/v2/reminder/bulk")
    Call<okhttp3.ResponseBody> setReminder(@Body ReminderPayload reminderPayload);
}

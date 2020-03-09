package com.netflix.conductor.core.execution.archival;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Headers;
import retrofit2.http.POST;

import java.util.List;
import java.util.Map;

public interface ReminderApi {

    @Headers({
            "Accept: text/plain",
            "Content-Type: application/json"
    })
    @POST("/v2/reminder/bulk")
    Call<okhttp3.ResponseBody> setReminder(@Body Map<String, List<ReminderPayload>> reminderPayload);
}

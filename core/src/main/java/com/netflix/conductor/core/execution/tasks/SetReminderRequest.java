package com.netflix.conductor.core.execution.tasks;

import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import okhttp3.ResponseBody;
import retrofit2.Call;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class SetReminderRequest {

    private final ExternalServiceConfiguration externalServiceConfiguration;
    private ReminderApi reminderApi;

    @Inject
    private SystemPropertiesConfiguration configuration;

    @Inject
    public SetReminderRequest(ExternalServiceConfiguration externalServiceConfiguration) {
        this.externalServiceConfiguration = externalServiceConfiguration;
        initialize();
    }

    private void initialize() {
        ApiProperties apiProperties = externalServiceConfiguration.getReminderEndpoint();
        this.reminderApi = externalServiceConfiguration.getReminderRetrofitBean(apiProperties).create(ReminderApi.class);
    }

    public RetrofitResponse<ResponseBody> setReminder(String workflowid) {
        Endpoint endpoint = new Endpoint(
                configuration.getProperty("flo.host","http://flo-server.swiggy.prod"),
                configuration.getProperty("flo.uri", "/api"),
                configuration.getProperty("flo.resource_id", "/workflow/" + workflowid));
        HttpNotification httpNotification = new HttpNotification("HTTP_DELETE", endpoint);
        ReminderPayload reminderPayload = new ReminderPayload(Integer.valueOf(configuration.getProperty("flo.ttl","10")),
                "FLO_ARCHIVAL", "FLO_ARCHIVAL", workflowid, httpNotification);
        Map<String, List<ReminderPayload>> payload = new HashMap<>();
        payload.put("reminders", Arrays.asList(reminderPayload));
        Call<ResponseBody> retrofitCall = reminderApi.setReminder(payload);
        return ReminderRetrofitUtil.executeCall(retrofitCall, "setReminder");
    }
}

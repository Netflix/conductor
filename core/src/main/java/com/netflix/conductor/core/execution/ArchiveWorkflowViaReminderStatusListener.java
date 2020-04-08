package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.core.execution.archival.*;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class ArchiveWorkflowViaReminderStatusListener implements WorkflowStatusListener {

    private static final Logger LOG = LoggerFactory.getLogger(ArchiveWorkflowViaReminderStatusListener.class);

    private final ExternalServiceConfiguration externalServiceConfiguration;
    private ReminderApi reminderApi;

    @Inject
    private SystemPropertiesConfiguration configuration;

    @Inject
    public ArchiveWorkflowViaReminderStatusListener(ExternalServiceConfiguration externalServiceConfiguration) {
        this.externalServiceConfiguration = externalServiceConfiguration;
        initialize();
    }

    private void initialize() {
        ApiProperties apiProperties = externalServiceConfiguration.getReminderEndpoint();
        this.reminderApi = externalServiceConfiguration.getReminderRetrofitBean(apiProperties).create(ReminderApi.class);
    }

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        LOG.debug("Workflow {} is completed", workflow.getWorkflowId());
        archiveWorkflow(workflow);
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        LOG.debug("Workflow {} is terminated", workflow.getWorkflowId());
        archiveWorkflow(workflow);
    }

    public void archiveWorkflow(Workflow workflow) {
        Endpoint endpoint = new Endpoint(
                configuration.getProperty("flo.host","flo-server.swiggy.prod"),
                configuration.getProperty("flo.uri", "/api"),
                configuration.getProperty("flo.resource_id", "/workflow/" + workflow.getWorkflowId() + "/remove"));
        HttpNotification httpNotification = new HttpNotification("HTTP_DELETE", endpoint);
        ReminderPayload reminderPayload = new ReminderPayload(Integer.valueOf(configuration.getProperty("flo.ttl","7200")),
                "FLO_ARCHIVAL", "FLO_ARCHIVAL", workflow.getWorkflowId(), httpNotification);
        Map<String, List<ReminderPayload>> payload = new HashMap<>();
        payload.put("reminders", Arrays.asList(reminderPayload));
        Call<ResponseBody> retrofitCall = reminderApi.setReminder(payload);
        RetrofitResponse<ResponseBody> response = ReminderRetrofitUtil.executeCall(retrofitCall, "setReminder");
        if (response != null && response.getData()!= null) {
            try {
                workflow.getOutput().put("archival_reminder_id", response.getData().string());
            } catch (IOException e) {
                LOG.error("Unable to set reminder for workflow " + workflow.getWorkflowId());
            }
        }
    }

}

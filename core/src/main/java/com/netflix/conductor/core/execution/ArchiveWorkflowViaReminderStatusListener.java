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
        archiveWorkflow(workflow.getWorkflowId());
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        LOG.debug("Workflow {} is terminated", workflow.getWorkflowId());
        archiveWorkflow(workflow.getWorkflowId());
    }

    public RetrofitResponse<ResponseBody> archiveWorkflow(String workflowid) {
        Endpoint endpoint = new Endpoint(
                configuration.getProperty("flo.host","flo-server.swiggy.prod"),
                configuration.getProperty("flo.uri", "/api"),
                configuration.getProperty("flo.resource_id", "/workflow/" + workflowid + "/remove"));
        HttpNotification httpNotification = new HttpNotification("HTTP_DELETE", endpoint);
        ReminderPayload reminderPayload = new ReminderPayload(Integer.valueOf(configuration.getProperty("flo.ttl","7200")),
                "FLO_ARCHIVAL", "FLO_ARCHIVAL", workflowid, httpNotification);
        Map<String, List<ReminderPayload>> payload = new HashMap<>();
        payload.put("reminders", Arrays.asList(reminderPayload));
        Call<ResponseBody> retrofitCall = reminderApi.setReminder(payload);
        return ReminderRetrofitUtil.executeCall(retrofitCall, "setReminder");
    }

}

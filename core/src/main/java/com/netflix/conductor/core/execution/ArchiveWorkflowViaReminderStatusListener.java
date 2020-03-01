package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.core.execution.tasks.Endpoint;
import com.netflix.conductor.core.execution.tasks.HttpNotification;
import com.netflix.conductor.core.execution.tasks.ReminderPayload;
import com.netflix.conductor.core.execution.tasks.SetReminderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ArchiveWorkflowViaReminderStatusListener implements WorkflowStatusListener {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveWorkflowViaReminderStatusListener.class);

    @Inject
    private SetReminderRequest setReminderRequest;

    @Inject
    private SystemPropertiesConfiguration configuration;

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        LOG.debug("Workflow {} is completed", workflow.getWorkflowId());
        Endpoint endpoint = new Endpoint(
                configuration.getProperty("flo.host","flo-swagger.u4.swiggyops.de"),
                configuration.getProperty("flo.uri", "/api"),
                configuration.getProperty("flo.resource_id", "/workflow/" + workflow.getWorkflowId()));
        HttpNotification httpNotification = new HttpNotification("HTTP_DELETE", endpoint);
        ReminderPayload reminderPayload = new ReminderPayload(Integer.valueOf(configuration.getProperty("flo.ttl","36000")),
                "FLO_ARCHIVAL", "FLO_ARCHIVAL", workflow.getWorkflowId(), httpNotification);
        setReminderRequest.setReminder(reminderPayload);
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        LOG.debug("Workflow {} is terminated", workflow.getWorkflowId());
        Endpoint endpoint = new Endpoint("flo-server.swiggy.prod"," /api", "/workflow/" + workflow.getWorkflowId());
        HttpNotification httpNotification = new HttpNotification("HTTP_GET", endpoint);
        ReminderPayload reminderPayload = new ReminderPayload(36000, "FLO_ARCHIVAL", "FLO_ARCHIVAL", workflow.getWorkflowId(), httpNotification);
        setReminderRequest.setReminder(reminderPayload);
    }

}

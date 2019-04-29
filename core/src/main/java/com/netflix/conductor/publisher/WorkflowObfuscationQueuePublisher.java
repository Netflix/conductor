package com.netflix.conductor.publisher;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.QueueDAO;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class WorkflowObfuscationQueuePublisher {

    private QueueDAO queueDAO;
    private Configuration configuration;
    private String workflowObfuscationQueueName;
    private Boolean workflowObfuscationEnabled;

    @Inject
    public WorkflowObfuscationQueuePublisher(QueueDAO queueDAO, Configuration configuration) {
        this.queueDAO = queueDAO;
        this.configuration = configuration;
        this.workflowObfuscationQueueName = configuration.getProperty("workflow.obfuscation.coordinator.queue.name", "_obfuscationQueue");
        this.workflowObfuscationEnabled = configuration.getBooleanProperty("workflow.obfuscation.enabled", false);
    }

    public void publish(String workflowId, WorkflowDef workflowDef) {
        if(isObfuscationEnabled(workflowDef)) {
            queueDAO.push(workflowObfuscationQueueName, workflowId, 0);
        }
    }

    public void publishAll(List<String> workflowIds, WorkflowDef workflowDef) {
        if(isObfuscationEnabled(workflowDef)) {
            workflowIds.forEach(id -> queueDAO.push(workflowObfuscationQueueName, id, 0));
        }
    }

    private boolean isObfuscationEnabled(WorkflowDef workflowDef) {
        return workflowDef.getObfuscationFields() != null
                && !workflowDef.getObfuscationFields().isEmpty()
                && workflowObfuscationEnabled;
    }
}

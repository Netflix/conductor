package com.netflix.conductor.publisher;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.QueueDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class WorkflowObfuscationQueuePublisher {

    private QueueDAO queueDAO;
    private Configuration configuration;
    private String workflowObfuscationQueueName;
    private Boolean workflowObfuscationEnabled;
    private static Logger LOGGER = LoggerFactory.getLogger(WorkflowObfuscationQueuePublisher.class);

    @Inject
    public WorkflowObfuscationQueuePublisher(QueueDAO queueDAO, Configuration configuration) {
        this.queueDAO = queueDAO;
        this.configuration = configuration;
        this.workflowObfuscationQueueName = configuration.getProperty("workflow.obfuscation.queue.name", "_obfuscationQueue");
        this.workflowObfuscationEnabled = configuration.getBooleanProperty("workflow.obfuscation.enabled", false);
    }

    /**
     * Publishes a workflowId on the obfuscationQueue if the workflow has fields to be obfuscated
     * and the obfuscation is enabled on system properties. This method is called on Workflow's completion and termination events.
     * @param workflowId  id of the workflow
     * @param workflowDef workflowDef of the current workflow
     */
    public void publish(String workflowId, WorkflowDef workflowDef) {
        if(isObfuscationEnabled(workflowDef)) {
            LOGGER.debug("publishing workflowId: {} on obfuscation queue: {}", workflowId, workflowObfuscationQueueName);
            queueDAO.push(workflowObfuscationQueueName, workflowId, 0);
        }
    }

    /**
     * Publishes a workflowId on the obfuscationQueue if the workflows has fields to be obfuscated. This method is called from
     * the obfuscateWorkflow resource.
     * and the obfuscation is enabled on system properties
     * @param workflowIds  ids of the workflows
     * @param workflowDef workflowDef of the current workflows
     */
    public void publishAll(List<String> workflowIds, WorkflowDef workflowDef) {
        if(isObfuscationEnabled(workflowDef)) {
            LOGGER.debug("publishing workflowIds: {} on obfuscation queue: {}", workflowIds, workflowObfuscationQueueName);
            workflowIds.forEach(id -> queueDAO.push(workflowObfuscationQueueName, id, 0));
        }
    }

    private boolean isObfuscationEnabled(WorkflowDef workflowDef) {
        return workflowDef.getObfuscationFields() != null
                && !workflowDef.getObfuscationFields().isEmpty()
                && workflowObfuscationEnabled;
    }
}

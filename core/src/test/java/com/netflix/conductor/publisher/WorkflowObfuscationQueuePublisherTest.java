package com.netflix.conductor.publisher;

import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.dao.QueueDAO;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class WorkflowObfuscationQueuePublisherTest {

    private QueueDAO queueDAO = mock(QueueDAO.class);
    private Configuration configuration = mock(Configuration.class);
    private WorkflowObfuscationQueuePublisher publisher;
    private WorkflowDef workflowDef;
    private static String workflowId = "workflowId";

    @Before
    public void setup() {
        when(configuration.getBooleanProperty("workflow.obfuscation.enabled", false)).thenReturn(true);
        when(configuration.getProperty("workflow.obfuscation.coordinator.queue.name", "_obfuscationQueue")).thenReturn("_obfuscationQueue");
        workflowDef = new WorkflowDef();
        workflowDef.setObfuscationFields(ImmutableMap.of("workflowField", singletonList("field")));
    }

    @Test
    public void should_publish_message_if_workflow_def_has_obfuscation_fields() {
        publisher = new WorkflowObfuscationQueuePublisher(queueDAO, configuration);
        publisher.publish(workflowId, workflowDef);

        Mockito.verify(queueDAO, times(1)).push("_obfuscationQueue", workflowId, 0);
    }

    @Test
    public void should_not_publish_message_if_workflow_def_has_no_obfuscation_fields() {
        publisher = new WorkflowObfuscationQueuePublisher(queueDAO, configuration);
        workflowDef.setObfuscationFields(null);

        publisher.publish(workflowId, workflowDef);

        Mockito.verify(queueDAO, times(0)).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void should_not_publish_message_if_workflow_def_has_empty_obfuscation_fields() {
        publisher = new WorkflowObfuscationQueuePublisher(queueDAO, configuration);
        workflowDef.setObfuscationFields(new HashMap<>());

        publisher.publish(workflowId, workflowDef);

        Mockito.verify(queueDAO, times(0)).push(anyString(), anyString(), anyLong());
    }

    @Test
    public void should_not_publish_message_if_workflow_obfuscation_is_disabled() {
        when(configuration.getBooleanProperty("workflow.obfuscation.enabled", false)).thenReturn(false);
        publisher = new WorkflowObfuscationQueuePublisher(queueDAO, configuration);

        publisher.publish(workflowId, workflowDef);

        Mockito.verify(queueDAO, times(0)).push(anyString(), anyString(), anyLong());
    }
}

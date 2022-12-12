package com.netflix.conductor.metrics;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.MetadataService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
public class WorkflowMonitorTest {

    @Mock private MetadataService metadataService;
    @Mock private QueueDAO queueDAO;
    @Mock private ExecutionDAOFacade executionDAOFacade;

    private WorkflowMonitor workflowMonitor;

    @Before
    public void beforeEach() {
        workflowMonitor = new WorkflowMonitor(
                metadataService,
                queueDAO,
                executionDAOFacade,
                1000,
                Set.of()
        );
    }

    private WorkflowDef makeDef(String name, int version, String ownerApp) {
        WorkflowDef wd = new WorkflowDef();
        wd.setName(name);
        wd.setVersion(version);
        wd.setOwnerApp(ownerApp);
        return wd;
    }

    @Test
    public void testPendingWorkflowDataMap() {
        WorkflowDef test1_1 = makeDef("test1", 1, null);
        WorkflowDef test1_2 = makeDef("test1", 2, "name1");

        WorkflowDef test2_1 = makeDef("test2", 1, "first");
        WorkflowDef test2_2 = makeDef("test2", 2, "mid");
        WorkflowDef test2_3 = makeDef("test2", 3, "last");

        final Map<String, String> mapping = workflowMonitor.getPendingWorkflowToOwnerAppMap(List.of(
                test1_1,
                test1_2,
                test2_1,
                test2_2,
                test2_3
        ));

        Assert.assertEquals(2, mapping.keySet().size());
        Assert.assertTrue(mapping.containsKey("test1"));
        Assert.assertTrue(mapping.containsKey("test2"));

        Assert.assertEquals("name1", mapping.get("test1"));
        Assert.assertEquals("last", mapping.get("test2"));
    }
}

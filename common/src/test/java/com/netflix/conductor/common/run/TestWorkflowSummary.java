package com.netflix.conductor.common.run;

import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestWorkflowSummary {

    @Test
    public void testJsonSerializing() throws Exception {
        Workflow workflow = new Workflow();
        WorkflowSummary workflowSummary = new WorkflowSummary(workflow);

        String json = JsonMapperProvider.objectMapper.writeValueAsString(workflowSummary);
        WorkflowSummary read = JsonMapperProvider.objectMapper.readValue(json, WorkflowSummary.class);
        assertNotNull(read);
    }

    @Test
    public void testWorkflowInputOutputSerializing() throws Exception {
        Workflow workflow = new Workflow();

        workflow.setInput(ImmutableMap.of("a", "b", "c", ImmutableMap.of("d", "e")));
        workflow.setOutput(ImmutableMap.of("f", "g", "h", ImmutableMap.of("i", "j")));

        String json = JsonMapperProvider.objectMapper.writeValueAsString(new WorkflowSummary(workflow));
        WorkflowSummary workflowSummary = JsonMapperProvider.objectMapper.readValue(json, WorkflowSummary.class);

        assertEquals("{\"a\":\"b\",\"c\":{\"d\":\"e\"}}", workflowSummary.getInput());
        assertEquals("{\"f\":\"g\",\"h\":{\"i\":\"j\"}}", workflowSummary.getOutput());
    }

}

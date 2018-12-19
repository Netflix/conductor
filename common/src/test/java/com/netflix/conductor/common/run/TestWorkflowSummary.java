package com.netflix.conductor.common.run;

import com.google.common.collect.ImmutableMap;
import com.netflix.conductor.common.utils.JsonParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestWorkflowSummary {

    @Test
    public void testJsonSerializing() throws Exception {
        Workflow workflow = new Workflow();
        WorkflowSummary workflowSummary = new WorkflowSummary(workflow);

        String json = JsonParser.toJson(workflowSummary);
        WorkflowSummary read = JsonParser.parseJson(json, WorkflowSummary.class);
        assertNotNull(read);
    }

    @Test
    public void testWorkflowInputOutputSerializing() throws Exception {
        Workflow workflow = new Workflow();

        workflow.setInput(ImmutableMap.of("a", "b", "c", ImmutableMap.of("d", "e")));
        workflow.setOutput(ImmutableMap.of("f", "g", "h", ImmutableMap.of("i", "j")));

        String json = JsonParser.toJson(new WorkflowSummary(workflow));
        WorkflowSummary workflowSummary = JsonParser.parseJson(json, WorkflowSummary.class);

        assertEquals("{\"a\":\"b\",\"c\":{\"d\":\"e\"}}", workflowSummary.getInput());
        assertEquals("{\"f\":\"g\",\"h\":{\"i\":\"j\"}}", workflowSummary.getOutput());
    }

}

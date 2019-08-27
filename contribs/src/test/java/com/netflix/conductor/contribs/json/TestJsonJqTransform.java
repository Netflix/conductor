package com.netflix.conductor.contribs.json;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestJsonJqTransform {
    @Test
    public void wrongInputCausesNoMessage() {
        final JsonJqTransform t = new JsonJqTransform();
        final Workflow w = new Workflow();
        final Task task = new Task();
        final Map<String, Object> inputData = new HashMap<>();
        inputData.put("queryExpression", "{officeID: (.inputJson.OIDs | unique)[], requestedIndicatorList: .inputJson.requestedindicatorList}");
        final Map<String, Object> inputJson = new HashMap<>();
        inputJson.put("OIDs", Collections.singletonList("VALUE"));
        final Map<String, Object> indicatorList = new HashMap<>();
        indicatorList.put("indicator", "AFA");
        indicatorList.put("value", false);
        inputJson.put("requestedindicatorList", Collections.singletonList(indicatorList));
        inputData.put("inputJson", inputJson);
        task.setInputData(inputData);
        task.setOutputData(new HashMap<>());

        t.start(w, task, null);

        Assert.assertEquals("Encountered \" \"|\" \"| \"\" at line 1, column 40.\n"
                + "Was expecting one of:\n"
                + "    \"}\" ...\n"
                + "    \",\" ...\n", task.getOutputData().get("error"));
    }
}

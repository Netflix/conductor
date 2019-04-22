package com.netflix.conductor.service;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;

public interface ObfuscationService {

    public void obfuscateFields(String workflowId, WorkflowDef workflowDef);

    public void obfuscateFieldsByWorkflowDef(String name, Integer version);

}

package com.netflix.conductor.obfuscation.service;

import com.netflix.conductor.obfuscation.exception.ObfuscationServiceException;

public interface ObfuscationService {

    /**
     * Obfuscates the desired workflow fields defined in the WorkflowDef.
     * @throws ObfuscationServiceException if the workflow or the workflowDef is not found,
     * also if any json parsing fails, this exception will be thrown.
     * @param workflowId  id of the workflow to be obfuscated
     */
    void obfuscateFields(String workflowId);
}

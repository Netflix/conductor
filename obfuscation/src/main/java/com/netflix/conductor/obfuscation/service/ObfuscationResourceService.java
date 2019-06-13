package com.netflix.conductor.obfuscation.service;

public interface ObfuscationResourceService {

    void obfuscateWorkflows(String name, Integer version);

}

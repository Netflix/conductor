package com.netflix.conductor.core.execution.tasks;

import com.fasterxml.jackson.annotation.JsonProperty;
public class Endpoint {

    private String host;

    private String uri;

    @JsonProperty("resource_id")
    private String resourceId;


    public Endpoint(String host, String uri, String resourceId) {
        this.host = host;
        this.uri = uri;
        this.resourceId = resourceId;
    }
}

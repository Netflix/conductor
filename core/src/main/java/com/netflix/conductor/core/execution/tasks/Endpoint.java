package com.netflix.conductor.core.execution.tasks;

import com.fasterxml.jackson.annotation.JsonProperty;
public class Endpoint {

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

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

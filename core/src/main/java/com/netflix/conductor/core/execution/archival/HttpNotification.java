package com.netflix.conductor.core.execution.archival;

public class HttpNotification {

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    private String type;

    private Endpoint endpoint;

    public HttpNotification(String type, Endpoint endpoint) {
        this.type = type;
        this.endpoint = endpoint;
    }

}

package com.netflix.conductor.core.execution.tasks;

public class HttpNotification {

    private String type;

    private Endpoint endpoint;

    public HttpNotification(String type, Endpoint endpoint) {
        this.type = type;
        this.endpoint = endpoint;
    }

}

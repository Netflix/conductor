package com.netflix.conductor.core.execution.tasks;


import java.io.Serializable;

public class ReminderPayload implements Serializable {

    private int sendAfter;

    private String to;

    private String from;

    private String group;

    private HttpNotification httpNotification;

    public int getSendAfter() {
        return sendAfter;
    }

    public void setSendAfter(int sendAfter) {
        this.sendAfter = sendAfter;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public HttpNotification getHttpNotification() {
        return httpNotification;
    }

    public void setHttpNotification(HttpNotification httpNotification) {
        this.httpNotification = httpNotification;
    }

    public ReminderPayload(int sendAfter, String to, String from, String group, HttpNotification httpNotification) {
        this.sendAfter = sendAfter;
        this.to = to;
        this.from = from;
        this.group = group;
        this.httpNotification = httpNotification;
    }

}

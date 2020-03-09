package com.netflix.conductor.core.execution.archival;

import java.io.Serializable;
import java.util.HashMap;

public class ReminderPayload implements Serializable {

    private int sendAfter;

    private String to;

    private String from;

    private String group;

    private HttpNotification notification;

    private Object msg;

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

    public HttpNotification getNotification() {
        return notification;
    }

    public void setNotification(HttpNotification notification) {
        this.notification = notification;
    }

    public Object getMsg() {
        return msg;
    }

    public void setMsg(Object msg) {
        this.msg = msg;
    }

    public ReminderPayload(int sendAfter, String to, String from, String group, HttpNotification notification) {
        this.sendAfter = sendAfter;
        this.to = to;
        this.from = from;
        this.group = group;
        this.notification = notification;
        this.msg = new HashMap<>();
    }

}

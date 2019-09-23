package com.netflix.conductor.dao.kafka.index.serialiser;

public class Record {

    String type;
    Object payload;

    public String getType() {
        return type;
    }

    public Object getPayload() {
        return payload;
    }

    public Record(String type, Object payload) {
        this.type = type;
        this.payload = payload;
    }
}
package com.netflix.conductor.dao.kafka.index.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Record {

    @JsonProperty
    String type;

    @JsonProperty
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
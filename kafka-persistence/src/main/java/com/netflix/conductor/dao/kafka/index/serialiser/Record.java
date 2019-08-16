package com.netflix.conductor.dao.kafka.index.serialiser;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    public static ObjectMapper objectMapper() {
        final ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        return om;
    }
}
package com.netflix.conductor.dao.kafka.index.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Record {
    @JsonProperty
    String documentType;
    @JsonProperty
    String operationType;
    @JsonProperty
    Object payload;

    public String getDocumentType() {
        return this.documentType;
    }

    public Object getPayload() {
        return this.payload;
    }
    public Object getOperationType() {
        return this.operationType;
    }

    public Record(String operationType, String documentType, Object payload) {
        this.operationType = operationType;
        this.documentType = documentType;
        this.payload = payload;
    }
}
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
        return documentType;
    }

    public Object getPayload() {
        return payload;
    }

    public String getOperationType() {
        return operationType;
    }

    public Record(String operationType, String documentType, Object payload) {
        this.operationType = operationType;
        this.documentType = documentType;
        this.payload = payload;
    }
}
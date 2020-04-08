package com.netflix.conductor.core.execution.archival;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class RetrofitResponse<T> {

    private int httpStatus;
    private T data;

    RetrofitResponse(int httpStatus, T data) {
        this.data = data;
        this.httpStatus = httpStatus;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public T getData() {
        return data;
    }

}

package com.netflix.conductor.core.execution.tasks;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class RetrofitResponse<T> {

    private int httpStatus;
    private T data;

    RetrofitResponse(int httpStatus, T data) {
        this.data = data;
        this.httpStatus = httpStatus;
    }

}

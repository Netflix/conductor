package com.netflix.conductor.core.execution.exceptions;

public class TimeoutException extends RuntimeException {

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(Exception exception) {
        super(exception);
    }

    public TimeoutException(String message, Exception exception) {
        super(message, exception);
    }
}
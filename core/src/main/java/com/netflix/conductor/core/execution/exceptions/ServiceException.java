package com.netflix.conductor.core.execution.exceptions;

public class ServiceException extends RuntimeException {

    public ServiceException(String message) {
        super(message);
    }

    public ServiceException(Exception exception) {
        super(exception);
    }

    public ServiceException(String message, Exception exception) {
        super(message, exception);
    }
}

package com.netflix.conductor.core.execution.exceptions;

public class ObjectMappingException extends RuntimeException {

    public ObjectMappingException(String message) {
        super(message);
    }

    public ObjectMappingException(Exception exception) {
        super(exception);
    }

    public ObjectMappingException(String message, Exception exception) {
        super(message, exception);
    }

}

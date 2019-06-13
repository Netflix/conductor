package com.netflix.conductor.obfuscation.exception;

public class ObfuscationServiceException extends RuntimeException {

    public ObfuscationServiceException(String message) {
        super(message);
    }

    public ObfuscationServiceException(String message, Throwable e) {
        super(message, e);
    }
}

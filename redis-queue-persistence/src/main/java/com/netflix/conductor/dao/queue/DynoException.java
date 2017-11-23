package com.netflix.conductor.dao.queue;

public class DynoException  extends RuntimeException {

    private static final long serialVersionUID = 3757459937536486618L;

    public DynoException() {
        super();
    }

    public DynoException(String message, Throwable cause) {
        super(message, cause);
    }

    public DynoException(String message) {
        super(message);
    }

    public DynoException(Throwable cause) {
        super(cause);
    }


}

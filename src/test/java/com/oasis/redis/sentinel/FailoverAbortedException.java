package com.oasis.redis.sentinel;

public class FailoverAbortedException extends RuntimeException {

    private static final long serialVersionUID = 8492120740424975158L;

    public FailoverAbortedException(String message) {
        super(message);
    }

    public FailoverAbortedException(Throwable cause) {
        super(cause);
    }

    public FailoverAbortedException(String message, Throwable cause) {
        super(message, cause);
    }

}

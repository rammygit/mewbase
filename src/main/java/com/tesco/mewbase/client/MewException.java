package com.tesco.mewbase.client;

/**
 * Created by tim on 24/09/16.
 */
public class MewException extends RuntimeException {

    private final String errorCode;

    public MewException(Exception e) {
        this(null, e, null);
    }

    public MewException(String message) {
        this(message, (String) null);
    }

    public MewException(String message, Throwable cause, String errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public MewException(String message, Throwable cause) {
        this(message, cause, null);
    }

    public MewException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
}

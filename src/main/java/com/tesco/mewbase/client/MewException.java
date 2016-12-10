package com.tesco.mewbase.client;

/**
 * Created by tim on 24/09/16.
 */
public class MewException extends RuntimeException {

    private final int errorCode;

    public MewException(Exception e) {
        this(null, e, 0);
    }

    public MewException(String message) {
        this(message, 0);
    }

    public MewException(String message, Throwable cause, int errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public MewException(String message, Throwable cause) {
        this(message, cause, 0);
    }

    public MewException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}

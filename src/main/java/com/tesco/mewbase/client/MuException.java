package com.tesco.mewbase.client;

/**
 * Created by tim on 24/09/16.
 */
public class MuException extends RuntimeException {

    private final String errorCode;

    public MuException(String message, Throwable cause, String errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public MuException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
}

package com.tesco.mewbase.log.impl.file;

import io.vertx.core.buffer.Buffer;

import java.util.concurrent.CompletableFuture;

/**
 * Abstracts out the low level file access so we can provide different implementations that use
 * different techniques e.g. using RandomAccessFile, Vert.x AsyncFile, Memory mapped files etc
 *
 * Created by tim on 11/10/16.
 */
public interface BasicFile {

    CompletableFuture<Void> append(Buffer buffer, int writePos);

    CompletableFuture<Void> read(Buffer buffer, int length, int readPos);

    CompletableFuture<Void> close();
}
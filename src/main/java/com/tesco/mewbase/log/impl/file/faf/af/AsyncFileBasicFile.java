package com.tesco.mewbase.log.impl.file.faf.af;

import com.tesco.mewbase.log.impl.file.BasicFile;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/10/16.
 */
public class AsyncFileBasicFile implements BasicFile {

    private final AsyncFile af;

    public AsyncFileBasicFile(AsyncFile af) {
        this.af = af;
    }

    @Override
    public synchronized CompletableFuture<Void> append(Buffer buffer, int writePos) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        af.write(buffer, writePos, ar -> {
            if (ar.succeeded()) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Void> read(Buffer buffer, int length, int readPos) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        af.read(buffer, 0, readPos, length, ar -> {
            if (ar.succeeded()) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }

    public void close() {
        af.close();
    }
}

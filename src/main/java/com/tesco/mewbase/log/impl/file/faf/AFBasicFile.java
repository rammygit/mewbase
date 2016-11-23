package com.tesco.mewbase.log.impl.file.faf;

import com.tesco.mewbase.log.impl.file.BasicFile;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/10/16.
 */
public class AFBasicFile implements BasicFile {

    private final static Logger log = LoggerFactory.getLogger(AFBasicFile.class);

    private final AsyncFile af;

    public AFBasicFile(AsyncFile af) {
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

    public CompletableFuture<Void> close() {
        AsyncResCF<Void> ar = new AsyncResCF<>();
        af.flush(res -> {
            if (res.succeeded()) {
                af.close(ar);
            } else {
                ar.completeExceptionally(res.cause());
            }
        });
        return ar;
    }
}

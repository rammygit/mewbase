package com.tesco.mewbase.log.impl.file.faf.af;

import com.tesco.mewbase.log.impl.file.BasicFile;
import com.tesco.mewbase.log.impl.file.FileAccessManager;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/10/16.
 */
public class AsyncFileFileAccessManager implements FileAccessManager {

    private final Vertx vertx;

    public AsyncFileFileAccessManager(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public CompletableFuture<BasicFile> openBasicFile(File file) {
        CompletableFuture<BasicFile> cf = new CompletableFuture<>();
        vertx.fileSystem().open(file.getPath(), new OpenOptions().setWrite(true), ar -> {
            if (ar.succeeded()) {
                cf.complete(new AsyncFileBasicFile(ar.result()));
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }

    @Override
    public void scheduleOp(Runnable runner) {
        vertx.runOnContext(v -> runner.run());
    }
}

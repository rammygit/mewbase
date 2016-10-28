package com.tesco.mewbase.log.impl.file.faf;

import com.tesco.mewbase.log.impl.file.BasicFile;
import com.tesco.mewbase.log.impl.file.FileAccess;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/10/16.
 */
public class AFFileAccess implements FileAccess {

    private final static Logger log = LoggerFactory.getLogger(AFFileAccess.class);


    private final Vertx vertx;

    public AFFileAccess(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public CompletableFuture<BasicFile> openBasicFile(File file) {
        CompletableFuture<BasicFile> cf = new CompletableFuture<>();
        vertx.fileSystem().open(file.getPath(), new OpenOptions().setWrite(true), ar -> {
            if (ar.succeeded()) {
                if (ar.result() == null) {
                    log.error("Succeeded in opening file but result is null!");
                }
                cf.complete(new AFBasicFile(ar.result()));
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

package com.tesco.mewbase.log.impl.file;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Abstracts out the low level file access
 * <p>
 * Created by tim on 11/10/16.
 */
public interface FileAccess {

    CompletableFuture<BasicFile> openBasicFile(File file);

    void scheduleOp(Runnable runner);

}

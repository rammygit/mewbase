package com.tesco.mewbase.log;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface LogManager {

    Log getLog(String channel);

    CompletableFuture<Log> createLog(String channel);

    CompletableFuture<Void> close();
}

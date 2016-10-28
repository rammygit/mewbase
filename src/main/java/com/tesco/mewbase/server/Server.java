package com.tesco.mewbase.server;

import com.tesco.mewbase.function.FunctionManager;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Server extends FunctionManager {


    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

}

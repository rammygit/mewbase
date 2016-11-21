package com.tesco.mewbase.server.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Created by tim on 21/11/16.
 */
public interface Transport {

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    void connectHandler(Consumer<TransportConnection> connectionHandler);
}

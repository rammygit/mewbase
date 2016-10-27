package com.tesco.mewbase.client;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Client {

    CompletableFuture<Connection> connect(ConnectionOptions connectionOptions);

    CompletableFuture<Void> close();

    // Also provide a sync API

    Connection connectSync(ConnectionOptions connectionOptions);

    void closeSync();


}

package com.tesco.mubase.client;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Client {

    CompletableFuture<Connection> connect(ConnectionOptions connectionOptions);

    void close();

}

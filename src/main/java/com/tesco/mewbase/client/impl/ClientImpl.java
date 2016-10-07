package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.Connection;
import com.tesco.mewbase.client.ConnectionOptions;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public class ClientImpl implements Client {

    private final boolean ownVertx;
    private final Vertx vertx;
    private final NetClient netClient;

    public ClientImpl() {
        this(Vertx.vertx(), true);
    }

    public ClientImpl(Vertx vertx) {
        this(vertx, false);
    }

    protected ClientImpl(Vertx vertx, boolean ownVertx) {
        this.vertx = vertx;
        NetClientOptions options = new NetClientOptions();
        this.netClient = vertx.createNetClient(options);
        this.ownVertx = ownVertx;
    }

    @Override
    public CompletableFuture<Connection> connect(ConnectionOptions connectionOptions) {
        CompletableFuture<Connection> cf = new CompletableFuture<>();
        netClient.connect(connectionOptions.getPort(), connectionOptions.getHost(), ar -> {
            if (ar.succeeded()) {
                ClientConnection conn = new ClientConnection(ar.result());
                conn.doConnect(cf);
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Void> close() {
        netClient.close();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        if (ownVertx) {
            vertx.close(ar -> {
                if (ar.succeeded()) {
                    cf.complete(null);
                } else {
                    cf.completeExceptionally(ar.cause());
                }
            });
        } else {
            cf.complete(null);
        }
        return cf;
    }

    @Override
    public Connection connectSync(ConnectionOptions connectionOptions) {
        return null;
    }

    @Override
    public void closeSync() {

    }

}

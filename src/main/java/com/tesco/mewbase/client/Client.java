package com.tesco.mewbase.client;

import com.tesco.mewbase.client.spi.ClientFactory;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Client {

    static Client newClient() {
        return factory.newClient();
    }

    static Client newClient(Vertx vertx) {
        return factory.newClient(vertx);
    }

    CompletableFuture<Connection> connect(ConnectionOptions connectionOptions);

    CompletableFuture<Void> close();

    // Also provide a sync API

    Connection connectSync(ConnectionOptions connectionOptions);

    void closeSync();

    ClientFactory factory = ServiceHelper.loadFactory(ClientFactory.class);

}

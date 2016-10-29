package com.tesco.mewbase.server;

import com.tesco.mewbase.function.FunctionManager;
import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.spi.VertxFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 21/09/16.
 */
public interface Server extends FunctionManager {

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

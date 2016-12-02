package com.tesco.mewbase.server.spi;

import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public interface ServerFactory {

    Server newServer(ServerOptions serverOptions);

    Server newServer(Vertx vertx, ServerOptions serverOptions);
}

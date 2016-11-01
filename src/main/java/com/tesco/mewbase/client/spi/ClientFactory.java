package com.tesco.mewbase.client.spi;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public interface ClientFactory {

    Client newClient();

    Client newClient(Vertx vertx);
}

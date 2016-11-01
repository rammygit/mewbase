package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.spi.ClientFactory;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;
import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public class ClientFactoryImpl implements ClientFactory {


    @Override
    public Client newClient() {
        return new ClientImpl();
    }

    @Override
    public Client newClient(Vertx vertx) {
        return new ClientImpl(vertx);
    }
}

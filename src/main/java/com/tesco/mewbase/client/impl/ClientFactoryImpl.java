package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.spi.ClientFactory;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public class ClientFactoryImpl implements ClientFactory {


    @Override
    public Client newClient(ClientOptions clientOptions) {
        return new ClientImpl(clientOptions);
    }

    @Override
    public Client newClient(Vertx vertx, ClientOptions clientOptions) {
        return new ClientImpl(vertx, clientOptions);
    }
}

package com.tesco.mewbase.client.spi;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import io.vertx.core.Vertx;

/**
 * Created by tim on 29/10/16.
 */
public interface ClientFactory {

    Client newClient(ClientOptions clientOptions);

    Client newClient(Vertx vertx, ClientOptions clientOptions);
}

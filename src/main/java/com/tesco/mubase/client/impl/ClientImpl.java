package com.tesco.mubase.client.impl;

import com.tesco.mubase.client.Client;
import com.tesco.mubase.client.Connection;
import com.tesco.mubase.client.ConnectionOptions;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public class ClientImpl implements Client {
    @Override
    public CompletableFuture<Connection> connect(ConnectionOptions connectionOptions) {
        return null;
    }
}

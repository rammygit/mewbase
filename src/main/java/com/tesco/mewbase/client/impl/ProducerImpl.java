package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Producer;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 24/09/16.
 */
public class ProducerImpl implements Producer {

    private final ClientConnection connection;
    private final String channel;
    private final int id;

    public ProducerImpl(ClientConnection connection, String channel, int id) {
        this.connection = connection;
        this.channel = channel;
        this.id = id;
    }

    @Override
    public boolean startTx() {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> commitTx() {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> abortTx() {
        return null;
    }

    @Override
    public CompletableFuture<Void> emit(BsonObject event) {
        return connection.doEmit(channel, id, event);
    }
}

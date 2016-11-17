package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Producer;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 24/09/16.
 */
public class ProducerImpl implements Producer {

    private final ClientImpl client;
    private final String channel;
    private final int id;

    public ProducerImpl(ClientImpl client, String channel, int id) {
        this.client = client;
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
    public CompletableFuture<Void> publish(BsonObject event) {
        return client.doPublish(channel, id, event);
    }

    @Override
    public void close() {
        client.removeProducer(id);
    }
}

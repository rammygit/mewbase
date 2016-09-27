package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Producer;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 24/09/16.
 */
public class ProducerImpl implements Producer {

    private final ClientConnectionImpl connection;
    private final String streamName;
    private final int id;

    public ProducerImpl(ClientConnectionImpl connection, String streamName, int id) {
        this.connection = connection;
        this.streamName = streamName;
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
    public CompletableFuture<Void> emit(String eventType, BsonObject event) {
        return connection.doEmit(streamName, id, eventType, event);
    }
}

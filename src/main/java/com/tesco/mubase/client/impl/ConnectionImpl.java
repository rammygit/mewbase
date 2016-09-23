package com.tesco.mubase.client.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.client.Connection;
import com.tesco.mubase.client.Producer;
import com.tesco.mubase.client.QueryResult;
import com.tesco.mubase.client.Subscription;
import com.tesco.mubase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public class ConnectionImpl implements Connection {

    @Override
    public Producer createProducer(String streamName) {
        return null;
    }

    @Override
    public Subscription subscribe(String serverURL, SubDescriptor descriptor) {
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> query(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<BsonObject> queryOne(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<Void> close() {
        return null;
    }
}

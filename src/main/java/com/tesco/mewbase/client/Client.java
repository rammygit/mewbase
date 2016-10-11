package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 21/09/16.
 */
public interface Client {

    CompletableFuture<Connection> connect(ConnectionOptions connectionOptions);

    CompletableFuture<Void> close();

    // Also provide a sync API

    Connection connectSync(ConnectionOptions connectionOptions);

    void closeSync();




}

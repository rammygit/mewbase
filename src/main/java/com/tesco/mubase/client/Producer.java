package com.tesco.mubase.client;

import com.tesco.mubase.common.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public interface Producer {

    boolean startTx();

    CompletableFuture<Boolean> emit(String eventType, BsonObject event);

    CompletableFuture<Boolean> commitTx();

    CompletableFuture<Boolean> abortTx();
}

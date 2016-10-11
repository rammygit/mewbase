package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.DocQuerier;
import com.tesco.mewbase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 22/09/16.
 */
public interface Connection extends DocQuerier {

    CompletableFuture<Subscription> subscribe(SubDescriptor subDescriptor);

    Producer createProducer(String channel);

    CompletableFuture<Void> emit(String channel, BsonObject event);

    CompletableFuture<Void> emit(String channel, BsonObject event, Function<BsonObject, String> partitionFunc);

    // Subscription

    CompletableFuture<Void> close();

    // Also provide sync API

    // TODO
}

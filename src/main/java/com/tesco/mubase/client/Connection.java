package com.tesco.mubase.client;

import com.tesco.mubase.common.BsonObject;
import com.tesco.mubase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public interface Connection {

    Producer createProducer(String streamName);


    // Subscription

    Subscription subscribe(String serverURL, SubDescriptor descriptor);


    // Documents

    CompletableFuture<QueryResult> query(String binderName, BsonObject matcher);


    CompletableFuture<Void> close();
}

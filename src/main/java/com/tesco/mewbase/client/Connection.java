package com.tesco.mewbase.client;

import com.tesco.mewbase.common.DocQuerier;
import com.tesco.mewbase.common.DocUpdater;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.function.FunctionManager;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public interface Connection extends DocQuerier {

    Producer createProducer(String streamName);

    // Subscription

    CompletableFuture<Subscription> subscribe(SubDescriptor descriptor);

    CompletableFuture<Void> close();

    // Also provide sync API

    // TODO
}

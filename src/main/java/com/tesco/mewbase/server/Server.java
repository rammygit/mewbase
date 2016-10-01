package com.tesco.mewbase.server;

import com.tesco.mewbase.client.Connection;
import com.tesco.mewbase.common.ReceivedEvent;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.function.FunctionManager;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Created by tim on 21/09/16.
 */
public interface Server extends FunctionManager {

    // Indexes

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

}

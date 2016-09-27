package com.tesco.mewbase.server;

import com.tesco.mewbase.common.FunctionContext;
import com.tesco.mewbase.common.ReceivedEvent;
import com.tesco.mewbase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Created by tim on 21/09/16.
 */
public interface Server {

    // Functions

    void installFunction(String functionName, SubDescriptor descriptor, BiConsumer<FunctionContext, ReceivedEvent> function);

    void uninstallFunction(String functionName);

    // Indexes

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    void createBinder(String binderName);

}

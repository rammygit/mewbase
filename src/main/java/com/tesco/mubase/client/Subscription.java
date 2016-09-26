package com.tesco.mubase.client;

import com.tesco.mubase.common.ReceivedEvent;

import java.util.function.Consumer;

/**
 * Created by tim on 22/09/16.
 */
public interface Subscription {

    void setHandler(Consumer<ReceivedEvent> handler);

    // TODO also provide RxJava Observable?

    // TODO return CompletableFuture<Void> ?
    void unsubscribe();
}

package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.common.ReceivedEvent;

import java.util.function.Consumer;

/**
 * Created by tim on 22/09/16.
 */
public interface Subscription {

    void setHandler(Consumer<ReceivedEvent> handler);

    void pause();

    void resume();

    void acknowledge();

    // TODO also provide RxJava Observable?

    // TODO return CompletableFuture<Void> ?

    void unsubscribe();

    // Blocking yuck!

    BsonObject receive(long timeout);
}

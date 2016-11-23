package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public interface Subscription {

    void pause();

    void resume();

    // TODO also provide RxJava Observable?

    // TODO return CompletableFuture<Void> ?

    /**
     * Unsubscribe a durable subscription
     */
    void unsubscribe();

    /**
     * Close the subscription
     */
    void close();

    // Blocking yuck!

    BsonObject receive(long timeout);
}

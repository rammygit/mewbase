package com.tesco.mubase.client;

import com.tesco.mubase.common.EventHolder;

import java.util.function.Consumer;

/**
 * Created by tim on 22/09/16.
 */
public interface Subscription {

    void setHandler(Consumer<EventHolder> handler);

    // TODO also provide RxJava Observable?

    void unsubscribe();
}

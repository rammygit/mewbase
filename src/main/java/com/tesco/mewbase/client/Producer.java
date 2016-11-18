package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * Used mainly if you want transactions
 * <p>
 * Created by tim on 22/09/16.
 */
public interface Producer extends Transactional {

    CompletableFuture<Void> publish(BsonObject event);

    void close();

    // TODO should also support flow control on publish. WriteStream? Reactive Streams?

}

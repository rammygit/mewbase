package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public interface Producer extends Transactional {

    CompletableFuture<Void> emit(BsonObject event);

    // TODO should also support flow control on emits. WriteStream? Reactive Streams?

}

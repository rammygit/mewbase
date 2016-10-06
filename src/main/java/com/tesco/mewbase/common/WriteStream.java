package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;
import io.vertx.core.Handler;

/**
 * Created by tim on 27/09/16.
 */
public interface WriteStream {

    void exceptionHandler(Handler<Throwable> handler);

    void write(BsonObject bsonObject);

    void close();

    boolean writeQueueFull();

    void drainHandler(Runnable handler);
}

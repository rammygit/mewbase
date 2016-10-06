package com.tesco.mewbase.log.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.WriteStream;
import io.vertx.core.Handler;

import java.util.Queue;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryWriteStream implements WriteStream {

    private final Queue<BsonObject> queue;

    public InMemoryWriteStream(Queue<BsonObject> queue) {
        this.queue = queue;
    }

    @Override
    public void exceptionHandler(Handler<Throwable> handler) {

    }

    @Override
    public void write(BsonObject bsonObject) {
        queue.add(bsonObject);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public void drainHandler(Runnable handler) {

    }
}

package com.tesco.mubase.log.impl.inmem;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.log.LogReadStream;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryLogReadStream implements LogReadStream {

    private Consumer<BsonObject> handler;
    private boolean paused;
    private Iterator<BsonObject> iter;
    private Context ctx;

    public InMemoryLogReadStream(long seqNo, Iterator<BsonObject> iter) {
        this.iter = iter;
        this.ctx = Vertx.currentContext();
        while (iter.hasNext()) {
            BsonObject obj = iter.next();
            long seq = obj.getLong("seqNo");
            if (seq == seqNo) {
                break;
            }
        }
    }

    private void deliverOnContext() {
        ctx.runOnContext(v -> {
            if (iter.hasNext()) {
                if (handler != null && !paused) {
                    BsonObject obj = iter.next();
                    handler.accept(obj);
                    if (!paused) {
                        deliverOnContext();
                    }
                }
            } else {
                paused = true;
            }
        });
    }

    @Override
    public void exceptionHandler(Handler<Throwable> handler) {

    }

    @Override
    public void handler(Consumer<BsonObject> handler) {
        this.handler = handler;
        deliverOnContext();
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        paused = false;
        deliverOnContext();
    }

    @Override
    public void close() {
        paused = true;
    }
}

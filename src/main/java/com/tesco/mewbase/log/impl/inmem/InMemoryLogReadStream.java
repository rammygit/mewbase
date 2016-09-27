package com.tesco.mewbase.log.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.log.LogReadStream;
import com.tesco.mewbase.server.impl.ServerConnectionImpl;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryLogReadStream implements LogReadStream {

    private final static Logger log = LoggerFactory.getLogger(InMemoryLogReadStream.class);


    private Consumer<BsonObject> handler;
    private boolean paused;
    private Iterator<BsonObject> iter;
    private Context ctx;

    public InMemoryLogReadStream(Iterator<BsonObject> iter) {
        this.iter = iter;
        this.ctx = Vertx.currentContext();
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
        log.trace("Pausing readstream");
        paused = true;
    }

    @Override
    public void resume() {
        paused = false;
        deliverOnContext();
    }

    @Override
    public void close() {
        log.trace("Closing readstream");
        paused = true;
    }
}

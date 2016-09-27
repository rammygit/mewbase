package com.tesco.mubase.log;

import com.tesco.mubase.bson.BsonObject;
import io.vertx.core.Handler;

import java.util.function.Consumer;

/**
 * Created by tim on 27/09/16.
 */
public interface LogReadStream {

    void exceptionHandler(Handler<Throwable> handler);

    void handler(Consumer<BsonObject> handler);

    void pause();

    void resume();

    void close();
}

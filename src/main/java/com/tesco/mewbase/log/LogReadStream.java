package com.tesco.mewbase.log;

import com.tesco.mewbase.bson.BsonObject;
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

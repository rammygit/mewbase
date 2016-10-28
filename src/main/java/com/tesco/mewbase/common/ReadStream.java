package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 27/09/16.
 */
public interface ReadStream {

    void exceptionHandler(Consumer<Throwable> handler);

    void handler(BiConsumer<Long, BsonObject> handler);

    void start();

    void pause();

    void resume();

    void close();
}

package com.tesco.mewbase.doc;

import com.tesco.mewbase.bson.BsonObject;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by tim on 27/09/16.
 */
public interface DocReadStream {

    void exceptionHandler(Consumer<Throwable> handler);

    void handler(Consumer<BsonObject> handler);

    void start();

    void pause();

    void resume();

    void close();

    boolean hasMore();
}

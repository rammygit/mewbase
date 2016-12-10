package com.tesco.mewbase.server.impl;

import io.vertx.core.buffer.Buffer;

import java.util.function.Consumer;

/**
 * Created by tim on 21/11/16.
 */
public interface TransportConnection {

    void write(Buffer buffer);

    void handler(Consumer<Buffer> handler);

    void close();
}

package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.FrameHandler;

/**
 * Created by tim on 24/09/16.
 */
public interface ServerFrameHandler extends FrameHandler {

    @Override
    default void handleResponse(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleSubResponse(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleRecev(int size, BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleQueryResult(int size, BsonObject frame) {
        throw new UnsupportedOperationException();
    }
}

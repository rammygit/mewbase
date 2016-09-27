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
    default void handleRecev(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleQueryResponse(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleQueryResult(BsonObject frame) {
        throw new UnsupportedOperationException();
    }
}

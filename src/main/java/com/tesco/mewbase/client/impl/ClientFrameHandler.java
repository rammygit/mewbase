package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.FrameHandler;

/**
 * Created by tim on 24/09/16.
 */
public interface ClientFrameHandler extends FrameHandler {

    @Override
    default void handleConnect(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handlePublish(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleStartTx(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleCommitTx(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleAbortTx(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleSubscribe(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleSubClose(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleUnsubscribe(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleAckEv(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleQuery(BsonObject frame) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void handleQueryAck(BsonObject frame) {
        throw new UnsupportedOperationException();
    }
}

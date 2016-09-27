package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 23/09/16.
 */
public interface FrameHandler {

    void handleConnect(BsonObject frame);

    void handleResponse(BsonObject frame);

    void handleEmit(BsonObject frame);

    void handleStartTx(BsonObject frame);

    void handleCommitTx(BsonObject frame);

    void handleAbortTx(BsonObject frame);

    void handleSubscribe(BsonObject frame);

    void handleUnsubscribe(BsonObject frame);

    void handleSubResponse(BsonObject frame);

    void handleRecev(BsonObject frame);

    void handleAckEv(BsonObject frame);

    void handleQuery(BsonObject frame);

    void handleQueryResponse(BsonObject frame);

    void handleQueryResult(BsonObject frame);

    void handleQueryAck(BsonObject frame);

    void handlePing(BsonObject frame);
}

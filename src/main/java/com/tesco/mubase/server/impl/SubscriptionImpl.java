package com.tesco.mubase.server.impl;

import com.tesco.mubase.bson.BsonObject;

/**
 * Created by tim on 26/09/16.
 */
public class SubscriptionImpl {


    private final StreamProcessor streamProcessor;
    private final ServerConnectionImpl connection;
    private final int idPerConn;

    public SubscriptionImpl(StreamProcessor streamProcessor, ServerConnectionImpl connection, int idPerConn) {
        this.streamProcessor = streamProcessor;
        this.connection = connection;
        this.idPerConn = idPerConn;
    }

    protected void close() {
        streamProcessor.removeSubScription(this);
    }

    protected void sendEvent(BsonObject frame) {
        frame = frame.copy();
        frame.put("subID", idPerConn);
        connection.writeNonResponse("RECEV", frame);
    }
}

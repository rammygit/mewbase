package com.tesco.mubase.server.impl;

import com.tesco.mubase.bson.BsonObject;

/**
 * Created by tim on 26/09/16.
 */
public class SubscriptionImpl {

    private final StreamProcessor streamProcessor;
    private final ServerConnectionImpl connection;

    public SubscriptionImpl(StreamProcessor streamProcessor, ServerConnectionImpl connection) {
        this.streamProcessor = streamProcessor;
        this.connection = connection;
    }

    protected void close() {
        streamProcessor.removeSubScription(this);
    }

    protected void sendEvent(BsonObject frame) {
        connection.writeNonResponse("RECEV", frame);
    }
}

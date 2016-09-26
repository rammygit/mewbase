package com.tesco.mubase.client.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.client.Subscription;
import com.tesco.mubase.common.ReceivedEvent;

import java.util.function.Consumer;

/**
 * Created by tim on 24/09/16.
 */
public class SubscriptionImpl implements Subscription {

    private final int id;
    private final String streamName;
    private final ClientConnectionImpl conn;
    private Consumer<ReceivedEvent> handler;

    public SubscriptionImpl(int id, String streamName, ClientConnectionImpl conn) {
        this.id = id;
        this.streamName = streamName;
        this.conn = conn;
    }

    @Override
    public void setHandler(Consumer<ReceivedEvent> handler) {
        this.handler = handler;
    }

    @Override
    public void unsubscribe() {
        handler = null;
        conn.doUnsubscribe(id);
    }

    protected void handleRecevFrame(BsonObject frame) {
        ReceivedEvent re = new ReceivedEventImpl(this, streamName, frame.getString("eventType"), frame.getLong("timestamp"),
                frame.getLong("seqNo"), frame.getBsonObject("event"));
        Consumer<ReceivedEvent> h = handler; // Copy ref to avoid race if handler is unregistered
        h.accept(re);
    }

    protected void acknowledge() {
        conn.doAckEv(id);
    }
}

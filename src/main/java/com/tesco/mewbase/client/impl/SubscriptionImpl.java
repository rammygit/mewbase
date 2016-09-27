package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Subscription;
import com.tesco.mewbase.common.ReceivedEvent;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Created by tim on 24/09/16.
 */
public class SubscriptionImpl implements Subscription {

    private final int id;
    private final String streamName;
    private final ClientConnectionImpl conn;
    private final Queue<ReceivedEvent> buffered = new LinkedList<>();
    private Consumer<ReceivedEvent> handler;
    private final Context ctx;

    public SubscriptionImpl(int id, String streamName, ClientConnectionImpl conn) {
        this.id = id;
        this.streamName = streamName;
        this.conn = conn;
        this.ctx = Vertx.currentContext();
    }

    @Override
    public synchronized void setHandler(Consumer<ReceivedEvent> handler) {
        this.handler = handler;
        if (handler != null && !buffered.isEmpty()) {
            ctx.runOnContext(v -> {
                checkContext();
                while (true) {
                    ReceivedEvent re = buffered.poll();
                    if (re != null) {
                        handler.accept(re);
                    } else {
                        break;
                    }
                }
            });
        }
    }

    @Override
    public void unsubscribe() {
        handler = null;
        conn.doUnsubscribe(id);
    }

    protected synchronized void handleRecevFrame(BsonObject frame) {
        checkContext();
        ReceivedEvent re = new ReceivedEventImpl(this, streamName, frame.getString("eventType"), frame.getLong("timestamp"),
                frame.getLong("seqNo"), frame.getBsonObject("event"));
        Consumer<ReceivedEvent> h = handler; // Copy ref to avoid race if handler is unregistered
        if (h == null || !buffered.isEmpty()) {
            buffered.add(re);
        } else {
            h.accept(re);
        }
    }

    protected void acknowledge() {
        conn.doAckEv(id);
    }

    // Sanity check - this should always be executed using the connection's context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context!");
        }
    }
}

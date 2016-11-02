package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Subscription;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.server.impl.Codec;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Created by tim on 24/09/16.
 */
public class SubscriptionImpl implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private final int id;
    private final String channel;
    private final ClientImpl conn;
    private final Consumer<Delivery> handler;
    private final Context ctx;
    private boolean unsubscribed;

    public SubscriptionImpl(int id, String channel, ClientImpl conn, Consumer<Delivery> handler) {
        this.id = id;
        this.channel = channel;
        this.conn = conn;
        this.handler = handler;
        this.ctx = Vertx.currentContext();
    }

    @Override
    public synchronized void unsubscribe() {
        conn.doUnsubscribe(id);
        unsubscribed = true;
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void acknowledge() {

    }

    @Override
    public BsonObject receive(long timeout) {
        return null;
    }

    protected synchronized void handleRecevFrame(BsonObject frame) {
        if (unsubscribed) {
            return;
        }
        checkContext();
        int sizeBytes = 1234; // FIXME
        Delivery re = new DeliveryImpl(this, channel, frame.getLong(Codec.RECEV_TIMESTAMP),
                frame.getLong(Codec.RECEV_POS), frame.getBsonObject(Codec.RECEV_EVENT), sizeBytes);
        handler.accept(re);
    }

    protected void acknowledge(int sizeBytes) {
        conn.doAckEv(id, sizeBytes);
    }

    // Sanity check - this should always be executed using the connection's context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context!");
        }
    }
}

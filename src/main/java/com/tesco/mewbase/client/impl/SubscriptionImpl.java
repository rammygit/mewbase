package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.ClientDelivery;
import com.tesco.mewbase.client.Subscription;
import com.tesco.mewbase.server.impl.Codec;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Created by tim on 24/09/16.
 */
public class SubscriptionImpl implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private final int id;
    private final String channel;
    private final ClientImpl client;
    private final Consumer<ClientDelivery> handler;
    private final Context ctx;
    private final Queue<ClientDelivery> buffered = new LinkedList<>();
    private boolean closed;
    private boolean paused;

    public SubscriptionImpl(int id, String channel, ClientImpl client, Consumer<ClientDelivery> handler) {
        this.id = id;
        this.channel = channel;
        this.client = client;
        this.handler = handler;
        this.ctx = Vertx.currentContext();
    }

    @Override
    public synchronized void unsubscribe() {
        client.doUnsubscribe(id);
        closed = true;
    }

    @Override
    public synchronized void close() {
        client.doSubClose(id);
        closed = true;
    }

    @Override
    public synchronized void pause() {
        paused = true;
    }

    @Override
    public synchronized void resume() {
        if (paused) {
            paused = false;
            while (!paused) {
                ClientDelivery del = buffered.poll();
                if (del == null) {
                    break;
                }
                handler.accept(del);
            }
        }
    }

    @Override
    public BsonObject receive(long timeout) {
        return null;
    }

    protected synchronized void handleRecevFrame(int size, BsonObject frame) {
        if (closed) {
            return;
        }
        checkContext();
        ClientDelivery delivery = new ClientDeliveryImpl(channel, frame.getLong(Codec.RECEV_TIMESTAMP),
                frame.getLong(Codec.RECEV_POS), frame.getBsonObject(Codec.RECEV_EVENT), this, size);
        if (!paused) {
            handler.accept(delivery);
        } else {
            buffered.add(delivery);
        }
    }

    protected void acknowledge(long pos, int sizeBytes) {
        client.doAckEv(id, pos, sizeBytes);
    }

    // Sanity check - this should always be executed using the connection's context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context!");
        }
    }
}

package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.Log;
import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 26/09/16.
 */
public class ChannelProcessor {

    private final String channel;
    private final Log log;
    private final Set<SubscriptionImpl> subscriptions = new ConcurrentHashSet<>();
    private long lastDeliveredPos;

    public ChannelProcessor(String channel, Log log) {
        this.channel = channel;
        this.log = log;
    }

    public String getChannel() {
        return channel;
    }

    protected CompletableFuture<Long> handleEmit(BsonObject event) {
        BsonObject frame = new BsonObject();
        frame.put(Codec.RECEV_TIMESTAMP, System.currentTimeMillis());
        frame.put(Codec.RECEV_EVENT, event);
        CompletableFuture<Long> cf = log.append(frame);
        cf.thenAccept(pos -> {
            try {
                // TODO consider using our own simpler CompletableFuture type internally (or Vert.x futures)
                for (SubscriptionImpl subscription : subscriptions) {
                    subscription.handleEvent(pos, frame);
                }
                lastDeliveredPos = pos;
            } catch (Exception e) {
                throw new MewException(e);
            }
        });
        return cf;
    }

    protected void addSubScription(SubscriptionImpl subscription) {
        subscriptions.add(subscription);
    }

    protected SubscriptionImpl createSubscription(ServerConnectionImpl conn, int idPerConn, long startSeq) {
        return new SubscriptionImpl(this, conn, idPerConn, log, startSeq, lastDeliveredPos);
    }

    protected void removeSubScription(SubscriptionImpl subscription) {
        subscriptions.remove(subscription);
    }
}

package com.tesco.mubase.server.impl;

import com.tesco.mubase.bson.BsonObject;
import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tim on 26/09/16.
 */
public class StreamProcessor {

    private final String streamName;
    private final AtomicLong eventSeq = new AtomicLong();
    private final Set<SubscriptionImpl> subscriptions = new ConcurrentHashSet<>();

    public StreamProcessor(String streamName) {
        this.streamName = streamName;
    }

    protected synchronized CompletableFuture<Void> handleEmit(String eventType, BsonObject event, String sessID) {
        // TODO persist event
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.complete(null);

        BsonObject frame = new BsonObject();
        frame.put("eventType", eventType);
        frame.put("streamName", streamName);
        frame.put("timestamp", System.currentTimeMillis());
        frame.put("seqNo", eventSeq.getAndIncrement());
        frame.put("event", event);

        for (SubscriptionImpl subscription: subscriptions) {
            subscription.sendEvent(frame);
        }


        return cf;
    }

    protected void addSubScription(SubscriptionImpl subscription) {
        subscriptions.add(subscription);
    }

    protected void removeSubScription(SubscriptionImpl subscription) {
        subscriptions.remove(subscription);
    }
}

package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.log.Log;
import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tim on 26/09/16.
 */
public class StreamProcessor {

    private final String streamName;
    private final Log log;
    private final AtomicLong eventSeq = new AtomicLong();
    private final Set<SubscriptionImpl> subscriptions = new ConcurrentHashSet<>();

    public StreamProcessor(String streamName, Log log) {
        this.streamName = streamName;
        this.log = log;
    }

    public String getStreamName() {
        return streamName;
    }

    protected CompletableFuture<Void> handleEmit(BsonObject event) {
        BsonObject frame = new BsonObject();
        frame.put("streamName", streamName);
        frame.put("timestamp", System.currentTimeMillis());
        frame.put("event", event);
        CompletableFuture<Void> cf;
        long seq;
        // Needs to be sync to ensure log stores events in strict seq order
        synchronized (this) {
            seq = eventSeq.getAndIncrement();
            frame.put("seqNo", seq);
            cf = log.append(frame);
        }
        cf.thenRun(() -> {
            for (SubscriptionImpl subscription: subscriptions) {
                subscription.handleEvent(seq, frame);
            }
        });

        return cf;
    }

    protected void addSubScription(SubscriptionImpl subscription) {
        subscriptions.add(subscription);
    }

    protected SubscriptionImpl createSubscription(ServerConnectionImpl conn, int idPerConn, long startSeq) {
        return new SubscriptionImpl(this, conn, idPerConn, log, startSeq, eventSeq.get());
    }

    protected void removeSubScription(SubscriptionImpl subscription) {
        subscriptions.remove(subscription);
    }
}

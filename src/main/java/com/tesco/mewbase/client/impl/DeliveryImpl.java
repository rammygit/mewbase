package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

/**
 * Created by tim on 24/09/16.
 */
public class DeliveryImpl implements Delivery {

    private final SubscriptionImpl sub;
    private final String channel;
    private final long timestamp;
    private final long sequenceNumber;
    private final BsonObject event;
    private final int sizeBytes;

    public DeliveryImpl(SubscriptionImpl sub, String channel, long timestamp, long sequenceNumber, BsonObject event,
                        int sizeBytes) {
        this.sub = sub;
        this.channel = channel;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.event = event;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public String channel() {
        return channel;
    }

    @Override
    public long timeStamp() {
        return timestamp;
    }

    @Override
    public long channelPos() {
        return sequenceNumber;
    }

    @Override
    public BsonObject event() {
        return event;
    }

    @Override
    public void acknowledge() {
        sub.acknowledge(sizeBytes);
    }
}

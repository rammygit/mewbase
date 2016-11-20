package com.tesco.mewbase.common.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

/**
 * Created by tim on 24/09/16.
 */
public class DeliveryImpl implements Delivery {

    private final String channel;
    private final long timestamp;
    private final long sequenceNumber;
    private final BsonObject event;

    public DeliveryImpl(String channel, long timestamp, long sequenceNumber, BsonObject event) {
        this.channel = channel;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.event = event;
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

}

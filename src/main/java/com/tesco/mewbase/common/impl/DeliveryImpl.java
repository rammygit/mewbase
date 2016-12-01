package com.tesco.mewbase.common.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;

/**
 * Created by tim on 24/09/16.
 */
public class DeliveryImpl implements Delivery {

    protected final String channel;
    protected final long timestamp;
    protected final long channelPos;
    protected final BsonObject event;

    public DeliveryImpl(String channel, long timestamp, long channelPos, BsonObject event) {
        this.channel = channel;
        this.timestamp = timestamp;
        this.channelPos = channelPos;
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
        return channelPos;
    }

    @Override
    public BsonObject event() {
        return event;
    }

}

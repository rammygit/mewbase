package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReceivedEvent;

/**
 * Created by tim on 24/09/16.
 */
public class ReceivedEventImpl implements ReceivedEvent {

    private final SubscriptionImpl sub;
    private final String streamName;
    private final long timestamp;
    private final long sequenceNumber;
    private final BsonObject event;
    private final int sizeBytes;

    public ReceivedEventImpl(SubscriptionImpl sub, String streamName, long timestamp, long sequenceNumber, BsonObject event,
                             int sizeBytes) {
        this.sub = sub;
        this.streamName = streamName;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.event = event;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public long timeStamp() {
        return timestamp;
    }

    @Override
    public long sequenceNumber() {
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

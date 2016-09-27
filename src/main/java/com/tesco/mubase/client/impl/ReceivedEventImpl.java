package com.tesco.mubase.client.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.common.ReceivedEvent;

/**
 * Created by tim on 24/09/16.
 */
public class ReceivedEventImpl implements ReceivedEvent {

    private final SubscriptionImpl sub;
    private final String streamName;
    private final String eventType;
    private final long timestamp;
    private final long sequenceNumber;
    private final BsonObject event;

    public ReceivedEventImpl(SubscriptionImpl sub, String streamName, String eventType, long timestamp, long sequenceNumber, BsonObject event) {
        this.sub = sub;
        this.streamName = streamName;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.event = event;
    }

    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public String eventType() {
        return eventType;
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
        sub.acknowledge();
    }
}

package com.tesco.mubase.common;

import com.tesco.mubase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public interface ReceivedEvent {

    // Metadata for event

    String streamName();

    String eventType();

    long timeStamp();

    long sequenceNumber();



    BsonObject event();

    void acknowledge();

}

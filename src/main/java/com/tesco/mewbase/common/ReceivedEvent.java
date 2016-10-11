package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public interface ReceivedEvent {

    // Metadata for event

    String channel();

    long timeStamp();

    long sequenceNumber();

    BsonObject event();

    void acknowledge();

}

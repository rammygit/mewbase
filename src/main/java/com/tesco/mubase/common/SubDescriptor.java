package com.tesco.mubase.common;

import com.tesco.mubase.bson.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public interface SubDescriptor {

    SubDescriptor setClientID(String clientID);

    SubDescriptor setStreamName(String streamName);

    SubDescriptor setEventType(String streamName);

    SubDescriptor setStartSequence(long sequence);

    SubDescriptor setStartTimestamp(long timestamp);

    SubDescriptor setEventMatcher(BsonObject matcher);
}

package com.tesco.mubase.common;

/**
 * Created by tim on 22/09/16.
 */
public interface EventHolder {

    String streamName();

    String eventType();

    long timeStamp();

    long subID(); // TODO Perhaps hide this?

    BsonObject event();

    void acknowledge();

}

package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Subscription;

/**
 * Created by tim on 22/09/16.
 */
public interface Delivery {

    // Metadata for event

    String channel();

    long timeStamp();

    long channelPos();

    BsonObject event();


}

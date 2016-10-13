package com.tesco.mewbase.log.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;

/**
 * Created by tim on 13/10/16.
 */
public class QueueEntry {
    final int receivedPos;
    final BsonObject bson;

    public QueueEntry(int receivedPos, BsonObject bson) {
        this.receivedPos = receivedPos;
        this.bson = bson;
    }
}

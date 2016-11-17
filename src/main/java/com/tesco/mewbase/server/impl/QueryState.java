package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.doc.DocReadStream;
import io.vertx.core.buffer.Buffer;

import java.util.function.Consumer;

/**
 * Created by tim on 17/11/16.
 */
public class QueryState implements Consumer<BsonObject> {

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable

    private final ConnectionImpl connection;
    private final int queryID;
    private final DocReadStream readStream;
    private int unackedBytes;

    public QueryState(ConnectionImpl connection, int queryID, DocReadStream readStream) {
        this.connection = connection;
        this.queryID = queryID;
        this.readStream = readStream;
    }

    void handleAck(int bytes) {
        connection.checkContext();
        unackedBytes -= bytes;
        // Low watermark to prevent thrashing
        if (unackedBytes < MAX_UNACKED_BYTES / 2) {
            readStream.resume();
        }
    }

    @Override
    public void accept(BsonObject doc) {
        connection.checkContext();
        boolean last = !readStream.hasMore();
        Buffer buff = connection.writeQueryResult(doc, queryID, last);
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            readStream.pause();
        }
        if (last) {
            connection.removeQueryState(queryID);
        }
    }
}
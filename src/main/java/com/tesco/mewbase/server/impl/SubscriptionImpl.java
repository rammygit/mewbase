package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogReadStream;
import com.tesco.mewbase.log.impl.file.FileLog;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 26/09/16.
 */
public class SubscriptionImpl extends SubscriptionBase {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable

    private final ConnectionImpl connection;
    private final int id;
    private int unackedBytes;

    public SubscriptionImpl(ConnectionImpl connection, int id, SubDescriptor subDescriptor) {
        super(connection.server(), subDescriptor);
        this.id = id;
        this.connection = connection;
    }

    @Override
    protected void onReceiveFrame(long pos, BsonObject frame) {
        frame = frame.copy();
        frame.put(Protocol.RECEV_SUBID, id);
        frame.put(Protocol.RECEV_POS, pos);
        Buffer buff = connection.writeNonResponse(Protocol.RECEV_FRAME, frame);
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            readStream.pause();
        }
    }

    protected void handleAckEv(long pos, int bytes) {
        checkContext();
        unackedBytes -= bytes;
        // Low watermark to prevent thrashing
        if (unackedBytes < MAX_UNACKED_BYTES / 2) {
            readStream.resume();
        }
        afterAcknowledge(pos);
    }


}

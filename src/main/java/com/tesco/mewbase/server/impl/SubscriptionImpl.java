package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.log.LogReadStream;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tim on 26/09/16.
 */
public class SubscriptionImpl {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable

    private final ConnectionImpl connection;
    private final int id;
    private final Context ctx;
    private final LogReadStream readStream;
    private int unackedBytes;

    public SubscriptionImpl(ConnectionImpl connection, int id,
                            LogReadStream readStream) {
        this.connection = connection;
        this.id = id;
        this.ctx = Vertx.currentContext();
        this.readStream = readStream;
        readStream.handler(this::handleEvent0);
        readStream.start();
    }

    protected void close() {
        checkContext();
        readStream.close();
    }

    // This can be called on different threads depending on whether the frame is coming from file or direct
    private synchronized void handleEvent0(long pos, BsonObject frame) {
        frame = frame.copy();
        frame.put(Codec.RECEV_SUBID, id);
        frame.put(Codec.RECEV_POS, pos);
        logger.trace("Writing recev");
        Buffer buff = connection.writeNonResponse(Codec.RECEV_FRAME, frame);
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            readStream.pause();
        }
    }

    protected void handleAckEv(int bytes) {
        checkContext();
        unackedBytes -= bytes;
        // Low watermark to prevent thrashing
        if (unackedBytes < MAX_UNACKED_BYTES / 2) {
            readStream.resume();
        }
    }

    // Sanity check - this should always be executed using the correct context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context! " + Vertx.currentContext() + " expected: " + ctx);
        }
    }
}

package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.log.Log;
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

    private final StreamProcessor streamProcessor;
    private final ServerConnectionImpl connection;
    private final int idPerConn;
    private final Log log;
    private long deliveredSeq;
    private long streamSeq;
    private ReadStream readStream;
    private boolean retro;
    private boolean paused;
    private int unackedBytes;
    private final Context ctx;

    public SubscriptionImpl(StreamProcessor streamProcessor, ServerConnectionImpl connection, int idPerConn,
                            Log log, long startSeq, long streamSeq) {
        this.streamProcessor = streamProcessor;
        this.connection = connection;
        this.idPerConn = idPerConn;
        this.log = log;
        this.ctx = Vertx.currentContext();
        this.streamSeq = streamSeq;
        if (startSeq != -1) {
            openReadStream(startSeq);
        }
    }

    protected void close() {
        checkContext();
        streamProcessor.removeSubScription(this);
    }

    protected void handleEvent(long seq, BsonObject frame) {
        checkContext();
        this.streamSeq = seq;
        if (retro || paused) {
            return;
        }
        handleEvent0(seq, frame);
    }

    private void handleEvent0(long seq, BsonObject frame) {
        logger.trace("sub for streamName {} id {} handling event with seq {}", streamProcessor.getStreamName(), idPerConn, seq);
        checkContext();
        frame = frame.copy();
        frame.put(Codec.RECEV_SUBID, idPerConn);
        Buffer buff = connection.writeNonResponse(Codec.RECEV_FRAME, frame);
        deliveredSeq = seq;
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            paused = true;
            if (readStream != null) {
                readStream.pause();
            }
        }
        if (retro && streamSeq == deliveredSeq) {
            readStream.close();
            retro = false;
        }
    }

    protected void handleAckEv(int bytes) {
        checkContext();
        unackedBytes -= bytes;
        // Low watermark to prevent thrashing
        if (unackedBytes < MAX_UNACKED_BYTES / 2) {
            paused = false;
            if (readStream != null) {
                readStream.resume();
            } else if (streamSeq > deliveredSeq) {
                // We've missed some messages
                openReadStream(deliveredSeq + 1);
            }
        }
    }

    private void openReadStream(long seqNo) {
        readStream = log.openReadStream(seqNo);
        retro = true;
        readStream.handler(bson -> handleEvent0(bson.getLong(Codec.RECEV_SEQNO), bson));
    }

    // Sanity check - this should always be executed using the connection's context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context!");
        }
    }
}

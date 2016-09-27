package com.tesco.mubase.server.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.log.Log;
import com.tesco.mubase.log.LogReadStream;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

/**
 * Created by tim on 26/09/16.
 */
public class SubscriptionImpl {

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable

    private final StreamProcessor streamProcessor;
    private final ServerConnectionImpl connection;
    private final int idPerConn;
    private final Log log;
    private long deliveredSeq;
    private long streamSeq;
    private LogReadStream readStream;
    private boolean retro;
    private boolean paused;
    private int unackedBytes;
    private final Context ctx;

    public SubscriptionImpl(StreamProcessor streamProcessor, ServerConnectionImpl connection, int idPerConn,
                            Log log, long startSeq) {
        this.streamProcessor = streamProcessor;
        this.connection = connection;
        this.idPerConn = idPerConn;
        this.log = log;
        this.ctx = Vertx.currentContext();
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
        checkContext();
        frame = frame.copy();
        frame.put("subID", idPerConn);
        Buffer buff = connection.writeNonResponse("RECEV", frame);
        deliveredSeq = seq;
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            paused = true;
            if (readStream != null) {
                readStream.pause();
            }
        }
        if (deliveredSeq == streamSeq && retro) {
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
        readStream.handler(bson -> handleEvent0(bson.getLong("seqNo"), bson));
    }

    // Sanity check - this should always be executed using the connection's context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context!");
        }
    }
}

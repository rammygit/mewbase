package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.log.Log;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 26/09/16.
 */
public class SubscriptionImpl {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable

    private final ChannelProcessor channelProcessor;
    private final ServerConnectionImpl connection;
    private final int idPerConn;
    private final Log log;
    private long deliveredPos;
    private long streamPos;
    private ReadStream readStream;
    private boolean retro;
    private boolean paused;
    private int unackedBytes;
    private final Context ctx;

    public SubscriptionImpl(ChannelProcessor channelProcessor, ServerConnectionImpl connection, int idPerConn,
                            Log log, long startSeq, long streamPos) {
        this.channelProcessor = channelProcessor;
        this.connection = connection;
        this.idPerConn = idPerConn;
        this.log = log;
        this.ctx = Vertx.currentContext();
        this.streamPos = streamPos;
        if (startSeq != -1) {
            openReadStream(startSeq);
        }
    }

    protected void close() {
        checkContext();
        channelProcessor.removeSubScription(this);
    }

    protected void handleEvent(long pos, BsonObject frame) {
        checkContext();
        this.streamPos = pos;
        if (retro || paused) {
            return;
        }
        handleEvent0(pos, frame);
    }

    private void handleEvent0(long pos, BsonObject frame) {
        logger.trace("sub for channel {} id {} handling event with seq {}", channelProcessor.getChannel(), idPerConn, pos);
        checkContext();
        frame = frame.copy();
        frame.put(Codec.RECEV_SUBID, idPerConn);
        frame.put(Codec.RECEV_POS, pos);
        Buffer buff = connection.writeNonResponse(Codec.RECEV_FRAME, frame);
        deliveredPos = pos;
        unackedBytes += buff.length();
        if (unackedBytes > MAX_UNACKED_BYTES) {
            paused = true;
            if (readStream != null) {
                readStream.pause();
            }
        }
        if (retro && streamPos == deliveredPos) {
            readStream.close();
            readStream = null;
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
            } else if (streamPos > deliveredPos) {
                // We've missed some messages
                openReadStream(deliveredPos + 1);
            }
        }
    }

    private void openReadStream(long pos) {
        CompletableFuture<ReadStream> cf = log.openReadStream(pos);
        retro = true;
        cf.handle((rs, t) -> {
            if (t != null) {
                logger.error("Failed to open readstream", t);
            } else {
                readStream = rs;
                readStream.handler(this::handleEvent0);
            }
           return null;
        });
    }

    // Sanity check - this should always be executed using the connection's context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context!");
        }
    }
}

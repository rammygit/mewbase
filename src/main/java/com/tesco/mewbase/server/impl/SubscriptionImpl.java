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
public class SubscriptionImpl {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private static final int MAX_UNACKED_BYTES = 4 * 1024 * 1024; // TODO make configurable


    private static final String DURABLE_SUBS_BINDER_LAST_ACKED_FIELD = "lastAcked";

    private final ConnectionImpl connection;
    private final int id;
    private final SubDescriptor subDescriptor;
    private final Context ctx;
    private LogReadStream readStream;
    private int unackedBytes;
    private boolean ignoreFirst;

    public SubscriptionImpl(ConnectionImpl connection, int id,
                            SubDescriptor subDescriptor) {
        this.connection = connection;
        this.id = id;
        this.subDescriptor = subDescriptor;
        this.ctx = Vertx.currentContext();
        if (subDescriptor.getDurableID() != null) {
            CompletableFuture<BsonObject> cf =
                    connection.server().docManager().get(ServerImpl.DURABLE_SUBS_BINDER_NAME, subDescriptor.getDurableID());
            cf.handle((doc, t) -> {
                if (t == null) {
                    if (doc != null) {
                        Long lastAcked = doc.getLong(DURABLE_SUBS_BINDER_LAST_ACKED_FIELD);
                        logger.trace("Restarting durable sub from (not including) {}", lastAcked);
                        if (lastAcked == null) {
                            throw new IllegalStateException("No last acked field");
                        } else {
                            subDescriptor.setStartPos(lastAcked);
                            // We don't want to redeliver the last acked event
                            ignoreFirst = true;
                        }
                    }
                    startReadStream();
                } else {
                    logger.error("Failed to lookup durable sub", t);
                }
                return null;
            }).exceptionally(t -> {
                logger.error("Failure in starting stream", t);
                return null;
            });
        } else {
            startReadStream();
        }
    }

    private void startReadStream() {
        Log log = connection.server().getLog(subDescriptor.getChannel());
        readStream = log.subscribe(subDescriptor);
        readStream.handler(this::handleEvent0);
        readStream.start();
    }

    protected void close() {
        checkContext();
        readStream.close();
    }

    // Unsubscribe deletes the durable subscription
    protected void unsubscribe() {
        if (subDescriptor.getDurableID() != null) {
            DocManager docManager = connection.server().docManager();
            docManager.delete(ServerImpl.DURABLE_SUBS_BINDER_NAME, subDescriptor.getDurableID());
        }
    }

    // This can be called on different threads depending on whether the frame is coming from file or direct
    private synchronized void handleEvent0(long pos, BsonObject frame) {
        if (ignoreFirst){
            ignoreFirst = false;
            return;
        }
        frame = frame.copy();
        frame.put(Codec.RECEV_SUBID, id);
        frame.put(Codec.RECEV_POS, pos);
        Buffer buff = connection.writeNonResponse(Codec.RECEV_FRAME, frame);
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
        // Store durable sub last acked position
        if (subDescriptor.getDurableID() != null) {
            BsonObject ackedDoc = new BsonObject().put(DURABLE_SUBS_BINDER_LAST_ACKED_FIELD, pos);
            DocManager docManager = connection.server().docManager();
            docManager.put(ServerImpl.DURABLE_SUBS_BINDER_NAME, subDescriptor.getDurableID(), ackedDoc);
        }
    }

    // Sanity check - this should always be executed using the correct context
    private void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context! " + Vertx.currentContext() + " expected: " + ctx);
        }
    }
}

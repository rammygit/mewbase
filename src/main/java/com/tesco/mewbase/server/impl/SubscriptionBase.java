package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogReadStream;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 26/09/16.
 */
public abstract class SubscriptionBase {

    private final static Logger logger = LoggerFactory.getLogger(SubscriptionImpl.class);

    private static final String DURABLE_SUBS_BINDER_LAST_ACKED_FIELD = "lastAcked";

    private final ServerImpl server;
    private final SubDescriptor subDescriptor;
    private final Context ctx;
    protected LogReadStream readStream;
    private boolean ignoreFirst;

    public SubscriptionBase(ServerImpl server,
                            SubDescriptor subDescriptor) {
        this.server = server;
        this.subDescriptor = subDescriptor;
        this.ctx = Vertx.currentContext();
        if (subDescriptor.getDurableID() != null) {
            CompletableFuture<BsonObject> cf =
                    server.docManager().get(ServerImpl.DURABLE_SUBS_BINDER_NAME, subDescriptor.getDurableID());
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
        Log log = server.getLog(subDescriptor.getChannel());
        readStream = log.subscribe(subDescriptor);
        readStream.handler(this::handleEvent0);
        readStream.start();
    }

    public void close() {
        checkContext();
        readStream.close();
    }

    // Unsubscribe deletes the durable subscription
    public void unsubscribe() {
        if (subDescriptor.getDurableID() != null) {
            DocManager docManager = server.docManager();
            docManager.delete(ServerImpl.DURABLE_SUBS_BINDER_NAME, subDescriptor.getDurableID());
        }
    }

    // This can be called on different threads depending on whether the frame is coming from file or direct
    private synchronized void handleEvent0(long pos, BsonObject frame) {
        if (ignoreFirst) {
            ignoreFirst = false;
            return;
        }
        onReceiveFrame(pos, frame);
    }

    protected abstract void onReceiveFrame(long pos, BsonObject frame);

    protected void afterAcknowledge(long pos) {
        // Store durable sub last acked position
        if (subDescriptor.getDurableID() != null) {
            BsonObject ackedDoc = new BsonObject().put(DURABLE_SUBS_BINDER_LAST_ACKED_FIELD, pos);
            DocManager docManager = server.docManager();
            docManager.put(ServerImpl.DURABLE_SUBS_BINDER_NAME, subDescriptor.getDurableID(), ackedDoc);
        }
    }

    // Sanity check - this should always be executed using the correct context
    protected void checkContext() {
        if (Vertx.currentContext() != ctx) {
            throw new IllegalStateException("Wrong context! " + Vertx.currentContext() + " expected: " + ctx);
        }
    }
}

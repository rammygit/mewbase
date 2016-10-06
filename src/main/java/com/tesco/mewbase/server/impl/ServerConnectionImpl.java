package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 23/09/16.
 */
public class ServerConnectionImpl implements ServerFrameHandler {

    private final static Logger log = LoggerFactory.getLogger(ServerConnectionImpl.class);

    private final ServerImpl server;
    private final NetSocket socket;
    private final Codec codec;
    private final Context context;
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new ConcurrentHashMap<>();
    private final PriorityQueue<WriteHolder> pq = new PriorityQueue<>();
    private boolean authorised;
    private int subSeq;
    private long writeSeq;
    private long expectedRespNo;

    public ServerConnectionImpl(ServerImpl server, NetSocket netSocket, Context context) {
        this.codec = new Codec(netSocket, this);
        this.server = server;
        this.socket = netSocket;
        this.context = context;
    }

    @Override
    public void handleConnect(BsonObject frame) {
        // TODO auth
        // TODO version checking
        authorised = true;
        BsonObject resp = new BsonObject();
        resp.put(Codec.RESPONSE_OK, true);
        writeResponse(Codec.RESPONSE_FRAME, resp, getWriteSeq());
    }

    @Override
    public void handleEmit(BsonObject frame) {
        checkAuthorised();
        String streamName = frame.getString(Codec.EMIT_STREAMNAME);
        BsonObject event = frame.getBsonObject(Codec.EMIT_EVENT);
        if (streamName == null) {
            logAndClose("No streamName in EMIT");
            return;
        }
        if (event == null) {
            logAndClose("No event in EMIT");
            return;
        }
        StreamProcessor processor = server.getStreamProcessor(streamName);
        long order = getWriteSeq();
        CompletableFuture<Void> cf = processor.handleEmit(event);

        cf.handle((v, ex) -> {
            BsonObject resp = new BsonObject();
            if (ex == null) {
                resp.put(Codec.RESPONSE_OK, true);
            } else {
                // TODO error code
                resp.put(Codec.RESPONSE_OK, false).put(Codec.RESPONSE_ERRMSG, "Failed to persist");
            }
            writeResponse(Codec.RESPONSE_FRAME, resp, order);
            return null;
        });

    }

    @Override
    public void handleStartTx(BsonObject frame) {
        checkAuthorised();
    }

    @Override
    public void handleCommitTx(BsonObject frame) {
        checkAuthorised();
    }

    @Override
    public void handleAbortTx(BsonObject frame) {
        checkAuthorised();
    }

    @Override
    public void handleSubscribe(BsonObject frame) {
        checkAuthorised();
        String streamName = frame.getString(Codec.SUBSCRIBE_STREAMNAME);
        if (streamName == null) {
            logAndClose("No streamName in SUBSCRIBE");
            return;
        }
        Long startSeq = frame.getLong(Codec.SUBSCRIBE_STARTSEQ);
        Long startTimestamp = frame.getLong(Codec.SUBSCRIBE_STARTTIMESTAMP);
        String durableID = frame.getString(Codec.SUBSCRIBE_DURABLEID);
        BsonObject matcher = frame.getBsonObject(Codec.SUBSCRIBE_MATCHER);
        int subID = subSeq++;
        checkWrap(subSeq);
        StreamProcessor processor = server.getStreamProcessor(streamName);
        SubscriptionImpl subscription = processor.createSubscription(this, subID, startSeq == null ? -1 : startSeq);
        subscriptionMap.put(subID, subscription);
        processor.addSubScription(subscription);
        BsonObject resp = new BsonObject();
        resp.put(Codec.RESPONSE_OK, true);
        resp.put(Codec.SUBRESPONSE_SUBID, subID);
        writeResponse(Codec.SUBRESPONSE_FRAME, resp, getWriteSeq());
        ServerConnectionImpl.log.trace("Subscribed streamName: {} startSeq {}", streamName, startSeq);
    }

    @Override
    public void handleUnsubscribe(BsonObject frame) {
        checkAuthorised();
        String subID = frame.getString(Codec.UNSUBSCRIBE_SUBID);
        if (subID == null) {
            logAndClose("No subID in UNSUBSCRIBE");
            return;
        }
        SubscriptionImpl subscription = subscriptionMap.remove(subID);
        if (subscription == null) {
            logAndClose("Invalid subID in UNSUBSCRIBE");
            return;
        }
        subscription.close();
        BsonObject resp = new BsonObject();
        resp.put(Codec.RESPONSE_OK, true);
        writeResponse(Codec.RESPONSE_FRAME, resp, getWriteSeq());
    }

    @Override
    public void handleAckEv(BsonObject frame) {
        checkAuthorised();
        String subID = frame.getString(Codec.ACKEV_SUBID);
        if (subID == null) {
            logAndClose("No subID in ACKEV");
            return;
        }
        Integer bytes = frame.getInteger(Codec.ACKEV_BYTES);
        if (bytes == null) {
            logAndClose("No bytes in ACKEV");
            return;
        }
        SubscriptionImpl subscription = subscriptionMap.get(subID);
        if (subscription == null) {
            logAndClose("Invalid subID in ACKEV");
            return;
        }
        subscription.handleAckEv(bytes);
    }

    @Override
    public void handleQuery(BsonObject frame) {
        checkAuthorised();
    }

    @Override
    public void handleQueryAck(BsonObject frame) {
        checkAuthorised();
    }

    @Override
    public void handlePing(BsonObject frame) {
        checkAuthorised();
    }

    protected Buffer writeNonResponse(String frameName, BsonObject frame) {
        Buffer buff = Codec.encodeFrame(frameName, frame);
        // TODO compare performance of writing directly in all cases and via context
        Context curr = Vertx.currentContext();
        if (curr != context) {
            context.runOnContext(v -> socket.write(buff));
        } else {
            socket.write(buff);
        }
        return buff;
    }

    protected void writeResponse(String frameName, BsonObject frame, long order) {
        Buffer buff = Codec.encodeFrame(frameName, frame);
        // TODO compare performance of writing directly in all cases and via context
        Context curr = Vertx.currentContext();
        if (curr != context) {
            context.runOnContext(v -> writeResponseOrdered(buff, order));
        } else {
            writeResponseOrdered(buff, order);
        }
    }

    protected void writeResponseOrdered(Buffer buff, long order) {
        // Writes can come in in the wrong order, we need to make sure they are written in the correct order
        if (order == expectedRespNo) {
            writeResponseOrdered0(buff);
        } else {
            // Out of order
            pq.add(new WriteHolder(order, buff));
            while (true) {
                WriteHolder head = pq.peek();
                if (head.order == expectedRespNo) {
                    pq.poll();
                    writeResponseOrdered0(buff);
                } else {
                    break;
                }
            }
        }
    }

    protected long getWriteSeq() {
        long seq = writeSeq;
        writeSeq++;
        checkWrap(writeSeq);
        return seq;
    }

    protected void checkWrap(int i) {
        // Sanity check - wrap around - won't happen but better to close connection that give incorrect behaviour
        if (i == Integer.MIN_VALUE) {
            String msg =  "int wrapped!";
            logAndClose(msg);
            throw new IllegalStateException(msg);
        }
    }

    protected void checkWrap(long l) {
        // Sanity check - wrap around - won't happen but better to close connection that give incorrect behaviour
        if (l == Long.MIN_VALUE) {
            String msg =  "long wrapped!";
            logAndClose(msg);
            throw new IllegalStateException(msg);
        }
    }

    protected void writeResponseOrdered0(Buffer buff) {
        socket.write(buff);
        expectedRespNo++;
        checkWrap(expectedRespNo);
    }

    protected void checkAuthorised() {
        if (!authorised) {
            log.error("Attempt to use unauthorised connection.");
        }
    }

    protected void logAndClose(String errMsg) {
        log.warn(errMsg + ". connection will be closed");
        close();
    }

    protected void close() {
        authorised = false;
        socket.close();
        server.removeConnection(this);
    }

    private static final class WriteHolder implements Comparable<WriteHolder> {
        final long order;
        final Buffer buff;

        public WriteHolder(long order, Buffer buff) {
            this.order = order;
            this.buff = buff;
        }

        @Override
        public int compareTo(WriteHolder other) {
            return Long.compare(this.order, other.order);
        }
    }

}

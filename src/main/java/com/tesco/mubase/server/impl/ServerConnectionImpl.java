package com.tesco.mubase.server.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.log.Log;
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
        resp.put("ok", true);
        writeResponse("RESPONSE", resp, getWriteSeq());
    }

    @Override
    public void handleEmit(BsonObject frame) {
        checkAuthorised();
        String streamName = frame.getString("streamName");
        String eventType = frame.getString("eventType");
        BsonObject event = frame.getBsonObject("event");
        if (streamName == null) {
            logAndClose("No streamName in EMIT");
            return;
        }
        if (eventType == null) {
            logAndClose("No eventType in EMIT");
            return;
        }
        if (event == null) {
            logAndClose("No event in EMIT");
            return;
        }
        StreamProcessor processor = server.getStreamProcessor(streamName);
        long order = getWriteSeq();
        CompletableFuture<Void> cf = processor.handleEmit(eventType, event);

        cf.handle((v, ex) -> {
            BsonObject resp = new BsonObject();
            if (ex == null) {
                resp.put("ok", true);
            } else {
                // TODO error code
                resp.put("ok", false).put("errMsg", "Failed to persist");
            }
            writeResponse("RESPONSE", resp, order);
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
        String streamName = frame.getString("streamName");
        String eventType = frame.getString("eventType");
        if (streamName == null) {
            logAndClose("No streamName in SUBSCRIBE");
            return;
        }
        Long startSeq = frame.getLong("startSeq");
        Long startTimestamp = frame.getLong("startTimestamp");
        String durableID = frame.getString("durableID");
        BsonObject matcher = frame.getBsonObject("matcher");
        int subID = subSeq++;
        checkWrap(subSeq);
        StreamProcessor processor = server.getStreamProcessor(streamName);
        Log log = server.getLog(streamName);
        SubscriptionImpl subscription =
                new SubscriptionImpl(processor, this, subID, log, startSeq == null ? -1 : startSeq);
        subscriptionMap.put(subID, subscription);
        processor.addSubScription(subscription);
        BsonObject resp = new BsonObject();
        resp.put("ok", true);
        resp.put("subID", subID);
        writeResponse("SUBRESPONSE", resp, getWriteSeq());
    }

    @Override
    public void handleUnsubscribe(BsonObject frame) {
        checkAuthorised();
        String subID = frame.getString("subID");
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
        resp.put("ok", true);
        writeResponse("RESPONSE", resp, getWriteSeq());
    }

    @Override
    public void handleAckEv(BsonObject frame) {
        checkAuthorised();
        String subID = frame.getString("subID");
        if (subID == null) {
            logAndClose("No subID in ACKEV");
            return;
        }
        Integer bytes = frame.getInteger("bytes");
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

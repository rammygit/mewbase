package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.auth.MewbaseAuthProvider;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.DocReadStream;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogReadStream;
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
public class ConnectionImpl implements ServerFrameHandler {

    private final static Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    private final ServerImpl server;
    private final NetSocket socket;
    private final Context context;
    private final DocManager docManager;
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new ConcurrentHashMap<>();
    private final PriorityQueue<WriteHolder> pq = new PriorityQueue<>();
    private final Map<Integer, QueryState> queryStates = new ConcurrentHashMap<>();

    private MewbaseAuthProvider authProvider;

    private boolean authenticated;
    private int subSeq;
    private long writeSeq;
    private long expectedRespNo;

    public ConnectionImpl(ServerImpl server, NetSocket netSocket, Context context, DocManager docManager) {
        netSocket.handler(new Codec(this).recordParser());
        this.server = server;
        this.socket = netSocket;
        this.context = context;
        this.docManager = docManager;
    }

    public ConnectionImpl(ServerImpl server, NetSocket netSocket, Context context, DocManager docManager, MewbaseAuthProvider authProvider) {
        netSocket.handler(new Codec(this).recordParser());
        this.server = server;
        this.socket = netSocket;
        this.context = context;
        this.docManager = docManager;
        this.authProvider = authProvider;
    }

    @Override
    public void handleConnect(BsonObject frame) {
        checkContext();

        BsonObject value = (BsonObject) frame.getValue(Codec.AUTH_INFO);

        authProvider.authenticate(value, res -> {
            if (res.getBoolean(Codec.RESPONSE_OK)) {
                authenticated = true;
            }
            writeResponse(Codec.RESPONSE_FRAME, res, getWriteSeq());
        });

        // TODO version checking
    }

    @Override
    public void handlePublish(BsonObject frame) {
        checkContext();
        checkAuthenticated();
        String channel = frame.getString(Codec.PUBLISH_CHANNEL);
        BsonObject event = frame.getBsonObject(Codec.PUBLISH_EVENT);
        if (channel == null) {
            logAndClose("No channel in PUB");
            return;
        }
        if (event == null) {
            logAndClose("No event in PUB");
            return;
        }
        long order = getWriteSeq();
        Log log = server.getLog(channel);
        BsonObject record = new BsonObject();
        record.put(Codec.RECEV_TIMESTAMP, System.currentTimeMillis());
        record.put(Codec.RECEV_EVENT, event);
        CompletableFuture<Long> cf = log.append(record);

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
        checkContext();
        checkAuthenticated();
    }

    @Override
    public void handleCommitTx(BsonObject frame) {
        checkContext();
        checkAuthenticated();
    }

    @Override
    public void handleAbortTx(BsonObject frame) {
        checkContext();
        checkAuthenticated();
    }

    @Override
    public void handleSubscribe(BsonObject frame) {
        checkContext();
        checkAuthenticated();
        String channel = frame.getString(Codec.SUBSCRIBE_CHANNEL);
        if (channel == null) {
            logAndClose("No channel in SUBSCRIBE");
            return;
        }
        Long startSeq = frame.getLong(Codec.SUBSCRIBE_STARTPOS);
        Long startTimestamp = frame.getLong(Codec.SUBSCRIBE_STARTTIMESTAMP);
        String durableID = frame.getString(Codec.SUBSCRIBE_DURABLEID);
        BsonObject matcher = frame.getBsonObject(Codec.SUBSCRIBE_MATCHER);
        SubDescriptor subDescriptor = new SubDescriptor().setStartPos(startSeq == null ? -1 : startSeq).setStartTimestamp(startTimestamp)
                .setMatcher(matcher).setDurableID(durableID).setChannel(channel);
        int subID = subSeq++;
        checkWrap(subSeq);
        Log log = server.getLog(channel);
        if (log == null) {
            // TODO send error back to client
            throw new IllegalStateException("No such channel " + channel);
        }
        LogReadStream readStream = log.subscribe(subDescriptor);
        SubscriptionImpl subscription = new SubscriptionImpl(this, subID, readStream);
        subscriptionMap.put(subID, subscription);
        BsonObject resp = new BsonObject();
        resp.put(Codec.RESPONSE_OK, true);
        resp.put(Codec.SUBRESPONSE_SUBID, subID);
        writeResponse(Codec.SUBRESPONSE_FRAME, resp, getWriteSeq());
        logger.trace("Subscribed channel: {} startSeq {}", channel, startSeq);
    }

    @Override
    public void handleUnsubscribe(BsonObject frame) {
        checkContext();
        checkAuthenticated();
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
        checkContext();
        checkAuthenticated();
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
        checkContext();
        checkAuthenticated();
        int queryID = frame.getInteger(Codec.QUERY_QUERYID);
        String docID = frame.getString(Codec.QUERY_DOCID);
        String binder = frame.getString(Codec.QUERY_BINDER);
        BsonObject matcher = frame.getBsonObject(Codec.QUERY_MATCHER);
        if (docID != null) {
            CompletableFuture<BsonObject> cf = docManager.get(binder, docID);
            cf.thenAccept(doc -> writeQueryResult(doc, queryID, true));
        } else if (matcher != null) {
            // TODO currently just use select all
            DocReadStream rs = docManager.getMatching(binder, doc -> true);
            QueryState holder = new QueryState(this, queryID, rs);
            rs.handler(holder);
            queryStates.put(queryID, holder);
            rs.start();
        }
    }

    @Override
    public void handleQueryAck(BsonObject frame) {
        checkContext();
        checkAuthenticated();
        Integer queryID = frame.getInteger(Codec.QUERYACK_QUERYID);
        if (queryID == null) {
            logAndClose("No queryID in QueryAck");
        } else {
            Integer bytes = frame.getInteger(Codec.QUERYACK_BYTES);
            if (bytes == null) {
                logAndClose("No bytes in QueryAck");
                return;
            }
            QueryState queryState = queryStates.get(queryID);
            if (queryState != null) {
                queryState.handleAck(bytes);
            }
        }
    }

    @Override
    public void handlePing(BsonObject frame) {
        checkContext();
        checkAuthenticated();
    }

    protected Buffer writeQueryResult(BsonObject doc, int queryID, boolean last) {
        BsonObject res = new BsonObject();
        res.put(Codec.QUERYRESULT_QUERYID, queryID);
        res.put(Codec.QUERYRESULT_RESULT, doc);
        res.put(Codec.QUERYRESULT_LAST, last);
        return writeNonResponse(Codec.QUERYRESULT_FRAME, res);
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
        checkContext();
        // Writes can come in in the wrong order, we need to make sure they are written in the correct order
        if (order == expectedRespNo) {
            writeResponseOrdered0(buff);
        } else {
            // Out of order
            pq.add(new WriteHolder(order, buff));
        }
        while (true) {
            WriteHolder head = pq.peek();
            if (head != null && head.order == expectedRespNo) {
                pq.poll();
                writeResponseOrdered0(head.buff);
            } else {
                break;
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
            String msg = "int wrapped!";
            logAndClose(msg);
            throw new IllegalStateException(msg);
        }
    }

    protected void checkWrap(long l) {
        // Sanity check - wrap around - won't happen but better to close connection that give incorrect behaviour
        if (l == Long.MIN_VALUE) {
            String msg = "long wrapped!";
            logAndClose(msg);
            throw new IllegalStateException(msg);
        }
    }

    protected void writeResponseOrdered0(Buffer buff) {
        socket.write(buff);
        expectedRespNo++;
        checkWrap(expectedRespNo);
    }

    protected void checkAuthenticated() {
        if (!authenticated) {
            throw new MewException("Attempt to use unauthenticated connection");
        }
    }

    protected void logAndClose(String errMsg) {
        logger.warn(errMsg + ". connection will be closed");
        close();
    }

    // Sanity check - this should always be executed using the correct context
    protected void checkContext() {
        if (Vertx.currentContext() != context) {
            throw new IllegalStateException("Wrong context!");
        }
    }

    protected void removeQueryState(int queryID) {
        queryStates.remove(queryID);
    }

    protected void close() {
        authenticated = false;
        socket.close();
        server.removeConnection(this);
        for (QueryState queryState : queryStates.values()) {
            queryState.close();
        }
    }

    private static final class WriteHolder implements Comparable<WriteHolder> {
        final long order;
        final Buffer buff;

        WriteHolder(long order, Buffer buff) {
            this.order = order;
            this.buff = buff;
        }

        @Override
        public int compareTo(WriteHolder other) {
            return Long.compare(this.order, other.order);
        }
    }


}

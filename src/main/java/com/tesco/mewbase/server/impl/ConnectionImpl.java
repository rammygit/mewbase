package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.DocReadStream;
import com.tesco.mewbase.log.Log;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 23/09/16.
 */
public class ConnectionImpl implements ServerFrameHandler {

    private final static Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);

    private final ServerImpl server;
    private final TransportConnection transportConnection;
    private final Context context;
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new HashMap<>();
    private final Map<Integer, QueryState> queryStates = new HashMap<>();
    private boolean authorised;
    private int subSeq;

    public ConnectionImpl(ServerImpl server, TransportConnection transportConnection, Context context) {
        Protocol protocol = new Protocol(this);
        RecordParser recordParser = protocol.recordParser();
        transportConnection.handler(recordParser::handle);
        this.server = server;
        this.transportConnection = transportConnection;
        this.context = context;
    }

    @Override
    public void handleConnect(BsonObject frame) {
        checkContext();
        // TODO auth
        // TODO version checking
        authorised = true;
        BsonObject resp = new BsonObject();
        resp.put(Protocol.RESPONSE_OK, true);
        writeResponse(Protocol.RESPONSE_FRAME, resp);
    }

    @Override
    public void handlePublish(BsonObject frame) {
        checkContext();
        checkAuthorised();
        String channel = frame.getString(Protocol.PUBLISH_CHANNEL);
        BsonObject event = frame.getBsonObject(Protocol.PUBLISH_EVENT);
        Integer sessID = frame.getInteger(Protocol.PUBLISH_SESSID);
        Integer requestID = frame.getInteger(Protocol.REQUEST_REQUEST_ID);
        if (channel == null) {
            missingField(Protocol.PUBLISH_CHANNEL, Protocol.PUBLISH_FRAME);
            return;
        }
        if (event == null) {
            missingField(Protocol.PUBLISH_EVENT, Protocol.PUBLISH_FRAME);
            return;
        }
        if (requestID == null) {
            missingField(Protocol.REQUEST_REQUEST_ID, Protocol.PUBLISH_FRAME);
            return;
        }
        Log log = server.getLog(channel);
        BsonObject record = new BsonObject();
        record.put(Protocol.RECEV_TIMESTAMP, System.currentTimeMillis());
        record.put(Protocol.RECEV_EVENT, event);
        CompletableFuture<Long> cf = log.append(record);

        cf.handle((v, ex) -> {
            BsonObject resp = new BsonObject();
            resp.put(Protocol.RESPONSE_REQUEST_ID, requestID);
            if (ex == null) {
                resp.put(Protocol.RESPONSE_OK, true);
            } else {
                // TODO error code
                resp.put(Protocol.RESPONSE_OK, false).put(Protocol.RESPONSE_ERRMSG, "Failed to persist");
            }
            writeResponse(Protocol.RESPONSE_FRAME, resp);
            return null;
        });
    }

    @Override
    public void handleStartTx(BsonObject frame) {
        checkContext();
        checkAuthorised();
    }

    @Override
    public void handleCommitTx(BsonObject frame) {
        checkContext();
        checkAuthorised();
    }

    @Override
    public void handleAbortTx(BsonObject frame) {
        checkContext();
        checkAuthorised();
    }

    @Override
    public void handleSubscribe(BsonObject frame) {
        checkContext();
        checkAuthorised();
        String channel = frame.getString(Protocol.SUBSCRIBE_CHANNEL);
        if (channel == null) {
            missingField(Protocol.SUBSCRIBE_CHANNEL, Protocol.SUBSCRIBE_FRAME);
            return;
        }
        Integer requestID = frame.getInteger(Protocol.REQUEST_REQUEST_ID);
        if (requestID == null) {
            missingField(Protocol.REQUEST_REQUEST_ID, Protocol.SUBSCRIBE_FRAME);
            return;
        }
        Long startSeq = frame.getLong(Protocol.SUBSCRIBE_STARTPOS);
        Long startTimestamp = frame.getLong(Protocol.SUBSCRIBE_STARTTIMESTAMP);
        String durableID = frame.getString(Protocol.SUBSCRIBE_DURABLEID);
        BsonObject matcher = frame.getBsonObject(Protocol.SUBSCRIBE_MATCHER);
        SubDescriptor subDescriptor = new SubDescriptor().setStartPos(startSeq == null ? -1 : startSeq).setStartTimestamp(startTimestamp)
                .setMatcher(matcher).setDurableID(durableID).setChannel(channel);
        int subID = subSeq++;
        checkWrap(subSeq);
        Log log = server.getLog(channel);
        if (log == null) {
            // TODO send error back to client
            throw new IllegalStateException("No such channel " + channel);
        }
        SubscriptionImpl subscription = new SubscriptionImpl(this, subID, subDescriptor);
        subscriptionMap.put(subID, subscription);
        BsonObject resp = new BsonObject();
        resp.put(Protocol.RESPONSE_REQUEST_ID, requestID);
        resp.put(Protocol.RESPONSE_OK, true);
        resp.put(Protocol.SUBRESPONSE_SUBID, subID);
        writeResponse(Protocol.SUBRESPONSE_FRAME, resp);
        logger.trace("Subscribed channel: {} startSeq {}", channel, startSeq);
    }

    @Override
    public void handleSubClose(BsonObject frame) {
        checkContext();
        checkAuthorised();
        Integer subID = frame.getInteger(Protocol.SUBCLOSE_SUBID);
        if (subID == null) {
            missingField(Protocol.SUBCLOSE_SUBID, Protocol.SUBCLOSE_FRAME);
            return;
        }
        Integer requestID = frame.getInteger(Protocol.REQUEST_REQUEST_ID);
        if (requestID == null) {
            missingField(Protocol.REQUEST_REQUEST_ID, Protocol.SUBCLOSE_FRAME);
            return;
        }
        SubscriptionImpl subscription = subscriptionMap.remove(subID);
        if (subscription == null) {
            invalidField(Protocol.SUBCLOSE_SUBID, Protocol.SUBCLOSE_FRAME);
            return;
        }
        subscription.close();
        BsonObject resp = new BsonObject();
        resp.put(Protocol.RESPONSE_OK, true);
        resp.put(Protocol.RESPONSE_REQUEST_ID, requestID);
        writeResponse(Protocol.RESPONSE_FRAME, resp);
    }

    @Override
    public void handleUnsubscribe(BsonObject frame) {
        checkContext();
        checkAuthorised();
        Integer subID = frame.getInteger(Protocol.UNSUBSCRIBE_SUBID);
        if (subID == null) {
            missingField(Protocol.UNSUBSCRIBE_SUBID, Protocol.UNSUBSCRIBE_FRAME);
            return;
        }
        Integer requestID = frame.getInteger(Protocol.REQUEST_REQUEST_ID);
        if (requestID == null) {
            missingField(Protocol.REQUEST_REQUEST_ID, Protocol.UNSUBSCRIBE_FRAME);
            return;
        }
        SubscriptionImpl subscription = subscriptionMap.remove(subID);
        if (subscription == null) {
            invalidField(Protocol.UNSUBSCRIBE_SUBID, Protocol.UNSUBSCRIBE_FRAME);
            return;
        }
        subscription.close();
        subscription.unsubscribe();
        BsonObject resp = new BsonObject();
        resp.put(Protocol.RESPONSE_OK, true);
        resp.put(Protocol.RESPONSE_REQUEST_ID, requestID);
        writeResponse(Protocol.RESPONSE_FRAME, resp);
    }

    @Override
    public void handleAckEv(BsonObject frame) {
        checkContext();
        checkAuthorised();
        Integer subID = frame.getInteger(Protocol.ACKEV_SUBID);
        if (subID == null) {
            missingField(Protocol.ACKEV_SUBID, Protocol.ACKEV_FRAME);
            return;
        }
        Integer bytes = frame.getInteger(Protocol.ACKEV_BYTES);
        if (bytes == null) {
            missingField(Protocol.ACKEV_BYTES, Protocol.ACKEV_FRAME);
            return;
        }
        Long pos = frame.getLong(Protocol.ACKEV_POS);
        if (pos == null) {
            missingField(Protocol.ACKEV_POS, Protocol.ACKEV_FRAME);
            return;
        }
        SubscriptionImpl subscription = subscriptionMap.get(subID);
        if (subscription == null) {
            invalidField(Protocol.ACKEV_SUBID, Protocol.ACKEV_FRAME);
            return;
        }
        subscription.handleAckEv(pos, bytes);
    }

    @Override
    public void handleQuery(BsonObject frame) {
        checkContext();
        checkAuthorised();
        Integer queryID = frame.getInteger(Protocol.QUERY_QUERYID);
        if (queryID == null) {
            missingField(Protocol.QUERY_QUERYID, Protocol.QUERY_FRAME);
            return;
        }
        String docID = frame.getString(Protocol.QUERY_DOCID);
        String binder = frame.getString(Protocol.QUERY_BINDER);
        BsonObject matcher = frame.getBsonObject(Protocol.QUERY_MATCHER);
        DocManager docManager = server().docManager();
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
        checkAuthorised();
        Integer queryID = frame.getInteger(Protocol.QUERYACK_QUERYID);
        if (queryID == null) {
            missingField(Protocol.QUERYACK_QUERYID, Protocol.QUERYACK_FRAME);
            return;
        }
        Integer bytes = frame.getInteger(Protocol.QUERYACK_BYTES);
        if (bytes == null) {
            missingField(Protocol.QUERYACK_BYTES, Protocol.QUERYACK_FRAME);
            return;
        }
        QueryState queryState = queryStates.get(queryID);
        if (queryState != null) {
            queryState.handleAck(bytes);
        }
    }

    @Override
    public void handlePing(BsonObject frame) {
        checkContext();
        checkAuthorised();
    }

    protected Buffer writeQueryResult(BsonObject doc, int queryID, boolean last) {
        BsonObject res = new BsonObject();
        res.put(Protocol.QUERYRESULT_QUERYID, queryID);
        res.put(Protocol.QUERYRESULT_RESULT, doc);
        res.put(Protocol.QUERYRESULT_LAST, last);
        return writeResponse(Protocol.QUERYRESULT_FRAME, res);
    }

    protected Buffer writeResponse(String frameName, BsonObject frame) {
        Buffer buff = Protocol.encodeFrame(frameName, frame);
        // TODO compare performance of writing directly in all cases and via context
        Context curr = Vertx.currentContext();
        if (curr != context) {
            context.runOnContext(v -> transportConnection.write(buff));
        } else {
            transportConnection.write(buff);
        }
        return buff;
    }


    protected void checkWrap(int i) {
        // Sanity check - wrap around - won't happen but better to close connection than give incorrect behaviour
        if (i == Integer.MIN_VALUE) {
            String msg = "int wrapped!";
            logger.error(msg);
            close();
        }
    }

    protected void checkAuthorised() {
        if (!authorised) {
            logger.error("Attempt to use unauthorised connection.");
        }
    }

    protected void missingField(String fieldName, String frameType) {
        logger.warn("protocol error: missing {} in {}. connection will be closed", fieldName, frameType);
        close();
    }

    protected void invalidField(String fieldName, String frameType) {
        logger.warn("protocol error: invalid {} in {}. connection will be closed", fieldName, frameType);
        close();
    }

    // Sanity check - this should always be executed using the correct context
    protected void checkContext() {
        if (Vertx.currentContext() != context) {
            throw new IllegalStateException("Wrong context!");
        }
    }

    protected void removeQueryState(int queryID) {
        checkContext();
        queryStates.remove(queryID);
    }

    protected void close() {
        checkContext();
        authorised = false;
        transportConnection.close();
        server.removeConnection(this);
        for (QueryState queryState : queryStates.values()) {
            queryState.close();
        }
    }

    protected ServerImpl server() {
        return server;
    }

}

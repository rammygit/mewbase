package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.*;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.impl.Codec;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 22/09/16.
 */
public class ClientImpl implements Client, ClientFrameHandler {

    private final static Logger log = LoggerFactory.getLogger(ClientImpl.class);

    private final AtomicInteger sessionSeq = new AtomicInteger();
    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final Map<Integer, ProducerImpl> producerMap = new ConcurrentHashMap<>();
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new ConcurrentHashMap<>();
    private final Queue<Consumer<BsonObject>> respQueue = new ConcurrentLinkedQueue<>();
    private final Map<Integer, CompletableFuture<BsonObject>> expectedQueryResults = new ConcurrentHashMap<>();
    private final Vertx vertx;
    private final NetClient netClient;
    private final ClientOptions clientOptions;
    private final boolean ownVertx;

    private NetSocket netSocket;
    private boolean connecting;
    private Queue<Buffer> bufferedWrites = new ConcurrentLinkedQueue<>();
    private Consumer<BsonObject> connectResponse;


    ClientImpl(ClientOptions clientOptions) {
        this(Vertx.vertx(), clientOptions, true);
    }

    ClientImpl(Vertx vertx, ClientOptions clientOptions) {
        this(vertx, clientOptions, false);
    }

    ClientImpl(Vertx vertx, ClientOptions clientOptions, boolean ownVertx) {
        this.vertx = vertx;
        this.netClient = vertx.createNetClient(clientOptions.getNetClientOptions());
        this.clientOptions = clientOptions;
        this.ownVertx = ownVertx;
    }

    @Override
    public Producer createProducer(String channel) {
        int id = sessionSeq.getAndIncrement();
        ProducerImpl prod = new ProducerImpl(this, channel, id);
        producerMap.put(id, prod);
        return prod;
    }

    @Override
    public CompletableFuture<Subscription> subscribe(SubDescriptor descriptor, Consumer<ClientDelivery> handler) {
        CompletableFuture<Subscription> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        if (descriptor.getChannel() == null) {
            throw new IllegalArgumentException("No channel in SubDescriptor");
        }
        frame.put(Codec.SUBSCRIBE_CHANNEL, descriptor.getChannel());
        frame.put(Codec.SUBSCRIBE_STARTPOS, descriptor.getStartPos());
        frame.put(Codec.SUBSCRIBE_STARTTIMESTAMP, descriptor.getStartTimestamp());
        frame.put(Codec.SUBSCRIBE_DURABLEID, descriptor.getDurableID());
        frame.put(Codec.SUBSCRIBE_MATCHER, descriptor.getMatcher());
        Buffer buffer = Codec.encodeFrame(Codec.SUBSCRIBE_FRAME, frame);
        write(cf, buffer, resp -> {
            boolean ok = resp.getBoolean(Codec.RESPONSE_OK);
            if (ok) {
                int subID = resp.getInteger(Codec.SUBRESPONSE_SUBID);
                SubscriptionImpl sub = new SubscriptionImpl(subID, descriptor.getChannel(), this, handler);
                subscriptionMap.put(subID, sub);
                cf.complete(sub);
            } else {
                cf.completeExceptionally(new MewException(resp.getString(Codec.RESPONSE_ERRMSG), resp.getString(Codec.RESPONSE_ERRCODE)));
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Void> publish(String channel, BsonObject event) {
        return doPublish(channel, -1, event);
    }

    @Override
    public CompletableFuture<Void> publish(String channel, BsonObject event, Function<BsonObject, String> partitionFunc) {
        // TODO partitions
        return doPublish(channel, -1, event);
    }

    @Override
    public CompletableFuture<BsonObject> findByID(String binderName, String id) {
        CompletableFuture<BsonObject> cf = new CompletableFuture<>();
        BsonObject matcher = new BsonObject().put("$match", new BsonObject().put("id", id));
        int queryID = nextQueryId.getAndIncrement();

        BsonObject frame = new BsonObject();
        frame.put(Codec.QUERY_QUERYID, queryID);
        frame.put(Codec.QUERY_BINDER, binderName);
        frame.put(Codec.QUERY_MATCHER, matcher);

        Buffer buffer = Codec.encodeFrame(Codec.QUERY_FRAME, frame);

        write(cf, buffer, resp -> {
            if (resp.getInteger(Codec.QUERYRESPONSE_QUERYID) != queryID) {
                cf.completeExceptionally(new IllegalStateException("Result query ID does not match handler expectation"));
                return;
            }

            Integer numResults = resp.getInteger(Codec.QUERYRESPONSE_NUMRESULTS);
            if (numResults == 1) {
                expectedQueryResults.put(queryID, cf);
            } else if (numResults == 0) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(new IllegalStateException("Invalid result count for get by ID"));
            }
        });

        return cf;
    }

    @Override
    public CompletableFuture<QueryResult> findMatching(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<Void> close() {
        netClient.close();
        if (ownVertx) {
            AsyncResCF<Void> cf = new AsyncResCF<>();
            vertx.close(cf);
            return cf;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    // FrameHandler

    @Override
    public void handleRecev(int size, BsonObject frame) {
        int subID = frame.getInteger(Codec.RECEV_SUBID);
        SubscriptionImpl sub = subscriptionMap.get(subID);
        if (sub == null) {
            // No subscription for this - maybe closed - ignore
        } else {
            sub.handleRecevFrame(size, frame);
        }
    }

    @Override
    public void handleQueryResponse(BsonObject frame) {
        handleResponse(frame);
    }

    @Override
    public void handleQueryResult(BsonObject frame) {
        int queryID = frame.getInteger(Codec.QUERYRESULT_QUERYID);
        CompletableFuture<BsonObject> cf = expectedQueryResults.get(queryID);
        if (cf != null) {
            cf.complete(frame.getBsonObject(Codec.QUERYRESULT_RESULT));
            doQueryAck(queryID);
            expectedQueryResults.remove(queryID);
        } else {
            throw new IllegalStateException("Received unexpected query result");
        }
    }

    @Override
    public void handlePing(BsonObject frame) {
    }

    @Override
    public void handleSubResponse(BsonObject frame) {
        handleResponse(frame);
    }

    @Override
    public void handleResponse(BsonObject frame) {
        if (connecting) {
            connectResponse.accept(frame);
        } else {
            Consumer<BsonObject> respHandler = respQueue.poll();
            if (respHandler == null) {
                throw new IllegalStateException("Unexpected response");
            }
            respHandler.accept(frame);
        }
    }

    // Must be synchronized to prevent interleaving
    protected synchronized void write(CompletableFuture cf, Buffer buff, Consumer<BsonObject> respHandler) {
        if (connecting || netSocket == null) {
            if (!connecting) {
                connect(cf);
            }
            bufferedWrites.add(buff);
        } else {
            netSocket.write(buff);
        }
        if (respHandler != null) {
            respQueue.add(respHandler);
        }
    }

    protected void write(Buffer buff) {
        if (netSocket == null) {
            throw new MewException("Not connected");
        }
        netSocket.write(buff);
    }

    private void connect(CompletableFuture cfConnect) {
        AsyncResCF<NetSocket> cf = new AsyncResCF<>();
        netClient.connect(clientOptions.getPort(), clientOptions.getHost(), cf);
        connecting = true;
        cf.thenAccept(ns -> sendConnect(cfConnect, ns)).exceptionally(t -> {
            cfConnect.completeExceptionally(t);
            return null;
        });
    }


    protected void doUnsubscribe(int subID) {
        subscriptionMap.remove(subID);
        BsonObject frame = new BsonObject();
        frame.put("SubID", subID);
        Buffer buffer = Codec.encodeFrame(Codec.UNSUBSCRIBE_FRAME, frame);
        write(buffer);
    }

    protected void doAckEv(int subID, int sizeBytes) {
        BsonObject frame = new BsonObject();
        frame.put(Codec.ACKEV_SUBID, subID);
        frame.put(Codec.ACKEV_BYTES, sizeBytes);
        Buffer buffer = Codec.encodeFrame(Codec.ACKEV_FRAME, frame);
        write(buffer);
    }

    protected void doQueryAck(int queryID) {
        BsonObject frame = new BsonObject();
        frame.put(Codec.QUERYACK_QUERYID, queryID);
        Buffer buffer = Codec.encodeFrame(Codec.QUERYACK_FRAME, frame);
        write(buffer);
    }

    protected CompletableFuture<Void> doPublish(String channel, int producerID, BsonObject event) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put(Codec.PUBLISH_CHANNEL, channel);
        frame.put(Codec.PUBLISH_SESSID, producerID);
        frame.put(Codec.PUBLISH_EVENT, event);
        Buffer buffer = Codec.encodeFrame(Codec.PUBLISH_FRAME, frame);
        write(cf, buffer, resp -> {
            boolean ok = resp.getBoolean(Codec.RESPONSE_OK);
            if (ok) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(new MewException(resp.getString(Codec.RESPONSE_ERRMSG), resp.getString(Codec.RESPONSE_ERRCODE)));
            }
        });
        return cf;
    }

    private synchronized void sendConnect(CompletableFuture cfConnect, NetSocket ns) {
        netSocket = ns;
        netSocket.handler(new Codec(this).recordParser());

        // Send the CONNECT frame
        BsonObject frame = new BsonObject();
        frame.put(Codec.CONNECT_VERSION, "0.1");
        Buffer buffer = Codec.encodeFrame(Codec.CONNECT_FRAME, frame);
        connectResponse = resp -> connected(cfConnect, resp);
        netSocket.write(buffer);
    }

    private synchronized void connected(CompletableFuture cfConnect, BsonObject resp) {
        connecting = false;
        boolean ok = resp.getBoolean(Codec.RESPONSE_OK);
        if (ok) {
            while (true) {
                Buffer buff = bufferedWrites.poll();
                if (buff == null) {
                    break;
                }
                netSocket.write(buff);
            }
        } else {
            cfConnect.completeExceptionally(new MewException(resp.getString(Codec.RESPONSE_ERRMSG),
                    resp.getString(Codec.RESPONSE_ERRCODE)));
        }
    }

}

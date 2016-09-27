package com.tesco.mubase.client.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.client.*;
import com.tesco.mubase.common.SubDescriptor;
import com.tesco.mubase.server.impl.Codec;
import com.tesco.mubase.common.FrameHandler;
import com.tesco.mubase.server.impl.ServerConnectionImpl;
import com.tesco.mubase.server.impl.ServerFrameHandler;
import io.vertx.core.buffer.Buffer;
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

/**
 * Created by tim on 22/09/16.
 */
public class ClientConnectionImpl implements Connection, ClientFrameHandler {

    private final static Logger log = LoggerFactory.getLogger(ClientConnectionImpl.class);


    private final AtomicInteger sessionSeq = new AtomicInteger();
    private final NetSocket netSocket;
    private final Codec codec;
    private final Map<Integer, ProducerImpl> producerMap = new ConcurrentHashMap<>();
    private final Map<Integer, SubscriptionImpl> subscriptionMap = new ConcurrentHashMap<>();
    private final Queue<Consumer<BsonObject>> respQueue = new ConcurrentLinkedQueue<>();

    public ClientConnectionImpl(NetSocket netSocket) {
        this.netSocket = netSocket;
        this.codec = new Codec(netSocket, this);
    }

    @Override
    public Producer createProducer(String streamName) {
        int id = sessionSeq.getAndIncrement();
        ProducerImpl prod = new ProducerImpl(this, streamName, id);
        producerMap.put(id, prod);
        return prod;
    }

    @Override
    public CompletableFuture<Subscription> subscribe(SubDescriptor descriptor) {
        CompletableFuture<Subscription> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        if (descriptor.getStreamName() == null) {
            throw new IllegalArgumentException("No streamName in SubDescriptor");
        }
        frame.put("streamName", descriptor.getStreamName());
        frame.put("eventType", descriptor.getEventType());
        frame.put("startSequence", descriptor.getStartSequence());
        frame.put("startTimestamp", descriptor.getStartTimestamp());
        frame.put("durableID", descriptor.getDurableID());
        frame.put("matcher", descriptor.getMatcher());
        Buffer buffer = Codec.encodeFrame("SUBSCRIBE", frame);
        log.trace("Writing subscribe");
        write(buffer, resp -> {
            boolean ok = resp.getBoolean("ok");
            if (ok) {
                int subID = resp.getInteger("subID");
                SubscriptionImpl sub = new SubscriptionImpl(subID, descriptor.getStreamName(), this);
                subscriptionMap.put(subID, sub);
                cf.complete(sub);
            } else {
                cf.completeExceptionally(new MuException(resp.getString("errMsg"), resp.getString("errCode")));
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<QueryResult> query(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<BsonObject> queryOne(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<Void> close() {
        return null;
    }

    // FrameHandler


    @Override
    public void handleRecev(BsonObject frame) {
        int subID = frame.getInteger("subID");
        SubscriptionImpl sub = subscriptionMap.get(subID);
        if (sub == null) {
            // No subscription for this - maybe closed - ignore
        } else {
            sub.handleRecevFrame(frame);
        }
    }

    @Override
    public void handleQueryResponse(BsonObject frame) {

    }

    @Override
    public void handleQueryResult(BsonObject frame) {

    }

    @Override
    public void handlePing(BsonObject frame) {

    }

    @Override
    public void handleSubResponse(BsonObject frame) {
        handleResponse(frame);
    }

    @Override
    public synchronized void handleResponse(BsonObject frame) {
        Consumer<BsonObject> respHandler = respQueue.poll();
        if (respHandler == null) {
            throw new IllegalStateException("Unexpected response");
        }
        respHandler.accept(frame);
    }

    // Must be synchronized to prevent interleaving
    protected synchronized void write(Buffer buff, Consumer<BsonObject> respHandler) {
        respQueue.add(respHandler);
        netSocket.write(buff);
        log.trace("Client writing buff of length " + buff.length());
    }


    protected void doUnsubscribe(int subID) {
        subscriptionMap.remove(subID);
        BsonObject frame = new BsonObject();
        frame.put("SubID", subID);
        Buffer buffer = Codec.encodeFrame("UNSUBSCRIBE", frame);
        netSocket.write(buffer);
    }

    protected synchronized void doConnect(CompletableFuture<Connection> cf) {
        BsonObject frame = new BsonObject();
        frame.put("version", "0.1");
        Buffer buffer = Codec.encodeFrame("CONNECT", frame);
        int len = buffer.getInt(0);
        log.trace("Length of connect buffer is " + len);

        write(buffer, resp -> {
            boolean ok = resp.getBoolean("ok");
            if (ok) {
                cf.complete(ClientConnectionImpl.this);
            } else {
                cf.completeExceptionally(new MuException(resp.getString("errMsg"), resp.getString("errCode")));
            }
        });
    }

    protected void doAckEv(int subID) {
        BsonObject frame = new BsonObject();
        frame.put("SubID", subID);
        Buffer buffer = Codec.encodeFrame("ACKEV", frame);
        netSocket.write(buffer);
    }

    protected CompletableFuture<Void> doEmit(String streamName, int producerID, String eventType, BsonObject event) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        BsonObject frame = new BsonObject();
        frame.put("streamName", streamName);
        frame.put("eventType", eventType);
        frame.put("sessID", producerID);
        frame.put("event", event);
        Buffer buffer = Codec.encodeFrame("EMIT", frame);
        write(buffer, resp -> {
            boolean ok = resp.getBoolean("ok");
            if (ok) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(new MuException(resp.getString("errMsg"), resp.getString("errCode")));
            }
        });
        return cf;
    }



}

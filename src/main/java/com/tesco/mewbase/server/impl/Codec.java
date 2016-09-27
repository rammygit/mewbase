package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.FrameHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * TODO we should write our own BSON parser and only decode fields when needed.
 * BSON is ordered and `type` should be the first field - we only need to decode that in order to know what to do
 * with the frame - in the case of an EMIT we should be able to pass the buffer direct to subscribers without
 * further decoding of the event, also to storage. I.e. we want to avoid decoding the entire frame in all cases
 *
 * Created by tim on 23/09/16.
 */
public class Codec {

    private final static Logger log = LoggerFactory.getLogger(Codec.class);

    private final FrameHandler frameHandler;

    public Codec(NetSocket socket, FrameHandler frameHandler) {
        this.frameHandler = frameHandler;
        RecordParser parser = RecordParser.newFixed(4, null);
        Handler<Buffer> handler = new Handler<Buffer>() {
            int size = -1;
            public void handle(Buffer buff) {
                if (size == -1) {
                    size = buff.getIntLE(0) - 4;
                    parser.fixedSizeMode(size);
                } else {
                    handleFrame(size, buff);
                    parser.fixedSizeMode(4);
                    size = -1;
                }
            }
        };
        parser.setOutput(handler);
        socket.handler(parser);
    }

    private void handleFrame(int size, Buffer buffer) {
        // TODO bit clunky - need to add size back in so it can be decoded, improve this!
        Buffer buff2 = Buffer.buffer(buffer.length() + 4);
        buff2.appendIntLE(size  + 4).appendBuffer(buffer);
        BsonObject bson = new BsonObject(buff2);
        handleBson(bson);
    }

    private void handleBson(BsonObject bson) {
        String type = bson.getString("type");
        BsonObject frame = bson.getBsonObject("frame");
        switch (type) {
            case "RESPONSE":
                frameHandler.handleResponse(frame);
                break;
            case "CONNECT":
                frameHandler.handleConnect(frame);
                break;
            case "EMIT":
                frameHandler.handleEmit(frame);
                break;
            case "STARTTX":
                frameHandler.handleStartTx(frame);
                break;
            case "COMMITTX":
                frameHandler.handleCommitTx(frame);
                break;
            case "ABORTTX":
                frameHandler.handleAbortTx(frame);
                break;
            case "SUBSCRIBE":
                frameHandler.handleSubscribe(frame);
                break;
            case "SUBRESPONSE":
                frameHandler.handleSubResponse(frame);
                break;
            case "RECEV":
                frameHandler.handleRecev(frame);
                break;
            case "ACKEV":
                frameHandler.handleAckEv(frame);
                break;
            case "QUERY":
                frameHandler.handleQuery(frame);
                break;
            case "QUERYRESPONSE":
                frameHandler.handleQueryResponse(frame);
                break;
            case "QUERYRESULT":
                frameHandler.handleQueryResult(frame);
                break;
            case "QUERYACK":
                frameHandler.handleQueryAck(frame);
                break;
            case "PING":
                frameHandler.handlePing(frame);
                break;
            default:
                log.error("Invalid frame type: " + type);
        }
    }

    public static Buffer encodeFrame(String frameType, BsonObject frame) {
        BsonObject env = new BsonObject();
        env.put("type", frameType).put("frame", frame);
        return env.encode();
    }
}

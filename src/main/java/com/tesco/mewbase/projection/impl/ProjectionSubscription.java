package com.tesco.mewbase.projection.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.impl.ServerImpl;
import com.tesco.mewbase.server.impl.SubscriptionBase;

import java.util.function.BiConsumer;

/**
 * Created by tim on 24/11/16.
 */
public class ProjectionSubscription extends SubscriptionBase {

    private static final int MAX_UNACKED_EVENTS = 1000; // TODO make configurable

    private final BiConsumer<Long, BsonObject> frameHandler;
    private int unackedEvents;

    public ProjectionSubscription(ServerImpl server, SubDescriptor subDescriptor,
                                  BiConsumer<Long, BsonObject> frameHandler) {
        super(server, subDescriptor);
        this.frameHandler = frameHandler;
    }

    @Override
    protected void onReceiveFrame(long pos, BsonObject frame) {
        unackedEvents++;
        if (unackedEvents > MAX_UNACKED_EVENTS) {
            readStream.pause();
        }
        frameHandler.accept(pos, frame);
    }

    void acknowledge(long pos) {
        unackedEvents--;
        // Low watermark to prevent thrashing
        if (unackedEvents < MAX_UNACKED_EVENTS / 2) {
            readStream.resume();
        }
        afterAcknowledge(pos);
    }

    void pause() {
        readStream.pause();
    }

    void resume() {
        readStream.resume();
    }
}

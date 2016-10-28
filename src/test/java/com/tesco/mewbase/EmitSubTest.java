package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.*;
import com.tesco.mewbase.client.impl.ClientImpl;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class EmitSubTest extends ServerTestBase {

    private final static Logger log = LoggerFactory.getLogger(EmitSubTest.class);

    @Test
    @Repeat(value = 10000)
    public void testSimpleEmitSubscribe(TestContext context) throws Exception {
        Connection conn = client.connect(new ConnectionOptions()).get();
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        Subscription sub = conn.subscribe(descriptor).get();
        Producer prod = conn.createProducer(TEST_CHANNEL_1);
        Async async = context.async();
        long now = System.currentTimeMillis();
        BsonObject sent = new BsonObject().put("foo", "bar");
        sub.setHandler(re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            context.assertEquals(0l, re.channelPos());
            context.assertTrue(re.timeStamp() >= now);
            BsonObject event = re.event();
            context.assertEquals(sent, event);
            async.complete();
        });
        prod.emit(sent).get();
    }

    @Test
    public void testSubscribeRetro(TestContext context) throws Exception {
        Connection conn = client.connect(new ConnectionOptions()).get();
        Producer prod = conn.createProducer(TEST_CHANNEL_1);
        int numEvents = 10;
        for (int i = 0; i < numEvents; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            CompletableFuture<Void> cf = prod.emit(event);
            if (i == numEvents - 1) {
                cf.get();
            }
        }
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setStartPos(0);
        Subscription sub = conn.subscribe(descriptor).get();
        Async async = context.async();
        AtomicLong lastPos = new AtomicLong(-1);
        AtomicInteger receivedCount = new AtomicInteger();
        sub.setHandler(re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            long last = lastPos.get();
            context.assertTrue(re.channelPos() > last);
            lastPos.set(re.channelPos());
            BsonObject event = re.event();
            long count = receivedCount.getAndIncrement();
            context.assertEquals(count, (long) event.getInteger("num"));
            if (count == numEvents - 1) {
                async.complete();
            }
        });

    }
}

package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.ClientDelivery;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.client.Subscription;
import com.tesco.mewbase.common.SubDescriptor;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class PubSubTest extends ServerTestBase {

    private final static Logger log = LoggerFactory.getLogger(PubSubTest.class);

    @Test
    //@Repeat(value = 10000)
    public void testSimplePubSub(TestContext context) throws Exception {
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = context.async();
        long now = System.currentTimeMillis();
        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            context.assertEquals(0l, re.channelPos());
            context.assertTrue(re.timeStamp() >= now);
            BsonObject event = re.event();
            context.assertEquals(sent, event);
            async.complete();
        };

        Subscription sub = client.subscribe(descriptor, handler).get();

        prod.publish(sent).get();
    }

    @Test
    //@Repeat(value = 10000)
    public void testSubscribeRetro(TestContext context) throws Exception {
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        int numEvents = 10;
        for (int i = 0; i < numEvents; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            CompletableFuture<Void> cf = prod.publish(event);
            if (i == numEvents - 1) {
                cf.get();
            }
        }
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setStartPos(0);

        Async async = context.async();
        AtomicLong lastPos = new AtomicLong(-1);
        AtomicInteger receivedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            long last = lastPos.get();
            context.assertTrue(re.channelPos() > last);
            lastPos.set(re.channelPos());
            BsonObject event = re.event();
            long count = receivedCount.getAndIncrement();
            context.assertEquals(count, (long)event.getInteger("num"));
            if (count == numEvents - 1) {
                async.complete();
            }
        };
        Subscription sub = client.subscribe(descriptor, handler).get();
    }
}

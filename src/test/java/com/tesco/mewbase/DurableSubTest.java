package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.*;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class DurableSubTest extends ServerTestBase {

    private final static Logger log = LoggerFactory.getLogger(DurableSubTest.class);

    private static final String TEST_DURABLE_ID = "testdurable";
    private static final int numEvents = 100;


    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        sendEvents(numEvents);
    }

    @Test
    //@Repeat(value = 10000)
    public void testSubscribeResubscribe(TestContext context) throws Exception {
        testSubscribeResubscribe(false, context);
    }

    @Test
    //@Repeat(value = 10000)
    public void testSubscribeResubscribeWithRestart(TestContext context) throws Exception {
        testSubscribeResubscribe(true, context);
    }

    private void testSubscribeResubscribe(boolean restart, TestContext context) throws Exception {

        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setDurableID(TEST_DURABLE_ID);
        descriptor.setStartPos(0);

        Async async1 = context.async();

        AtomicInteger expectedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler1 = re -> {
            int cnt = re.event().getInteger("count");

            context.assertEquals(expectedCount.getAndIncrement(), cnt);

            if (cnt < numEvents / 2) {
                re.acknowledge();
            } else if (cnt == numEvents / 2) {
                async1.complete();
            }
        };

        Subscription sub = client.subscribe(descriptor, handler1).get();
        async1.await();

        // Wait a little bit so as many acks as possible make it back to the server
        Thread.sleep(100);

        sub.close();

        if (restart) {

            client.close().get();

            server.stop().get();

            ServerOptions serverOptions = createServerOptions(logDir);
            ClientOptions clientOptions = createClientOptions();

            server = Server.newServer(vertx, serverOptions);
            server.start().get();

            client = Client.newClient(vertx, clientOptions);
        }

        Async async2 = context.async();

        AtomicInteger expectedCount2 = new AtomicInteger(-1);

        Consumer<ClientDelivery> handler2 = re -> {

            int cnt = re.event().getInteger("count");

            if (expectedCount2.get() == -1) {
                // First time - initialise the counter
                expectedCount2.set(cnt);
            }

            // We may get some redeliveries that's fine and too be expected

            context.assertEquals(expectedCount2.getAndIncrement(), cnt);

            re.acknowledge();
            if (cnt == numEvents - 1) {
                async2.complete();
            }
        };

        client.subscribe(descriptor, handler2).get();
        async2.await();

    }

    @Test
    public void testNewSubFromPos(TestContext context) throws Exception {

        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setStartPos(0);

        int startNum = 10;

        // Get the start pos
        Async async1 = context.async();
        AtomicLong startPos = new AtomicLong();
        Consumer<ClientDelivery> handler1 = re -> {
            int cnt = re.event().getInteger("count");
            if (cnt == startNum) {
                startPos.set(re.channelPos());
                async1.complete();
            }
        };
        Subscription sub = client.subscribe(descriptor, handler1).get();
        async1.await();
        sub.close();

        SubDescriptor descriptor2 = new SubDescriptor();
        descriptor2.setChannel(TEST_CHANNEL_1);
        descriptor2.setDurableID(TEST_DURABLE_ID);
        descriptor2.setStartPos(startPos.get());

        Async async2 = context.async();
        AtomicInteger expectedCount = new AtomicInteger(startNum);
        Consumer<ClientDelivery> handler2 = re -> {
            int cnt = re.event().getInteger("count");
            context.assertEquals(expectedCount.getAndIncrement(), cnt);
            if (cnt == startNum) {
                context.assertEquals(startPos.get(), re.channelPos());
            }
            if (cnt == numEvents - 1) {
                async2.complete();
            }
        };
        client.subscribe(descriptor2, handler2).get();
        async2.await();
    }

    @Test
    public void testUnsubscribe(TestContext context) throws Exception {

        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setDurableID(TEST_DURABLE_ID);
        descriptor.setStartPos(0);

        Async async1 = context.async();
        AtomicInteger expectedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler1 = re -> {
            int cnt = re.event().getInteger("count");
            context.assertEquals(expectedCount.getAndIncrement(), cnt);
            if (cnt < numEvents / 2) {
                re.acknowledge();
            } else if (cnt == numEvents / 2) {
                async1.complete();
            }
        };


        Subscription sub = client.subscribe(descriptor, handler1).get();
        async1.await();

        sub.unsubscribe(); // This should delete the durable sub

        // Should get all the events again
        Async async2 = context.async();
        AtomicInteger expectedCount2 = new AtomicInteger();
        Consumer<ClientDelivery> handler2 = re -> {
            int cnt = re.event().getInteger("count");
            context.assertEquals(expectedCount2.getAndIncrement(), cnt);
            re.acknowledge();
            if (cnt == numEvents - 1) {
                async2.complete();
            }
        };

        sub = client.subscribe(descriptor, handler2).get();
        async1.await();
    }

    private void sendEvents(int numEvents) throws Exception {
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        for (int i = 0; i < numEvents; i++) {
            BsonObject event = new BsonObject().put("count", i);
            prod.publish(event).get();
        }
    }


}

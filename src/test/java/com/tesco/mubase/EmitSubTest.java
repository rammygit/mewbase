package com.tesco.mubase;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.client.*;
import com.tesco.mubase.client.impl.ClientImpl;
import com.tesco.mubase.common.SubDescriptor;
import com.tesco.mubase.server.Server;
import com.tesco.mubase.server.ServerOptions;
import com.tesco.mubase.server.impl.ServerImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class EmitSubTest {

    private final static Logger log = LoggerFactory.getLogger(EmitSubTest.class);

    private static final String TEST_STREAM1 = "com.tesco.basket";
    private static final String TEST_EVENT_TYPE1 = "addItem";

    private Server server;
    private Client client;

    @Before
    public void before(TestContext context) throws Exception {
        server = new ServerImpl(new ServerOptions());
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();
        client = new ClientImpl();
    }

    @After
    public void after(TestContext context) throws Exception {
        client.close().get();
        server.stop().get();
    }

    @Test
    public void testSimpleEmitSubscribe(TestContext context) throws Exception {
        Connection conn = client.connect(new ConnectionOptions()).get();
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setStreamName(TEST_STREAM1);
        Subscription sub = conn.subscribe(descriptor).get();
        Producer prod = conn.createProducer(TEST_STREAM1);
        Async async = context.async();
        long now = System.currentTimeMillis();
        BsonObject sent = new BsonObject().put("foo", "bar");
        sub.setHandler(re -> {
            context.assertEquals(TEST_STREAM1, re.streamName());
            context.assertEquals(TEST_EVENT_TYPE1, re.eventType());
            context.assertEquals(0l, re.sequenceNumber());
            context.assertTrue(re.timeStamp() >= now);
            BsonObject event = re.event();
            context.assertEquals(sent, event);
            async.complete();
        });
        prod.emit(TEST_EVENT_TYPE1, sent).get();
    }
}

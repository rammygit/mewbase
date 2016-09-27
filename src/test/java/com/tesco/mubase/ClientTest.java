package com.tesco.mubase;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.client.*;
import com.tesco.mubase.client.impl.ClientImpl;
import com.tesco.mubase.common.SubDescriptor;
import com.tesco.mubase.server.Server;
import com.tesco.mubase.server.ServerOptions;
import com.tesco.mubase.server.impl.ServerImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * Created by tim on 26/09/16.
 */
public class ClientTest {

    private final static Logger log = LoggerFactory.getLogger(ClientTest.class);

    private static final String TEST_STREAM1 = "com.tesco.basket";
    private static final String TEST_EVENT_TYPE1 = "addItem";

    @Test
    public void testSimpleSubscribe() throws Exception {
        Server server = new ServerImpl(new ServerOptions());
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();
        Client client = new ClientImpl();
        CompletableFuture<Connection> cfConn = client.connect(new ConnectionOptions());
        CompletableFuture<Subscription> cfSub = cfConn.thenCompose(connection -> {
            SubDescriptor descriptor = new SubDescriptor();
            descriptor.setStreamName(TEST_STREAM1);
            CompletableFuture<Subscription> subCf = connection.subscribe(descriptor);
            return subCf;
        });
        Connection conn = cfConn.get();
        Subscription sub = cfSub.get();
        Producer prod = conn.createProducer(TEST_STREAM1;
        CountDownLatch latch = new CountDownLatch(1);
        sub.setHandler(re -> {
            latch.countDown();
        });
        BsonObject event = new BsonObject().put("foo", "bar");
        CompletableFuture<Void> cfEmit = prod.emit(TEST_EVENT_TYPE1, event);
        cfEmit.get();
        latch.await();
    }
}

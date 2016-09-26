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

    @Test
    public void testSimpleSubscribe() throws Exception {
        Server server = new ServerImpl(new ServerOptions());
        server.start();

        Thread.sleep(1000);

        Client client = new ClientImpl();
        CompletableFuture<Connection> cfConn = client.connect(new ConnectionOptions());
        CompletableFuture<Subscription> cfSub = cfConn.thenCompose(connection -> {
            SubDescriptor descriptor = new SubDescriptor();
            descriptor.setStreamName("somestream");
            CompletableFuture<Subscription> subCf = connection.subscribe("someurl??", descriptor);
            return subCf;
        });
        Connection conn = cfConn.get();
        Subscription sub = cfSub.get();
        Producer prod = conn.createProducer("somestream");
        CountDownLatch latch = new CountDownLatch(1);
        sub.setHandler(re -> {
            log.trace("Got received event: " + re);
            latch.countDown();
        });

        BsonObject event = new BsonObject().put("foo", "bar");
        CompletableFuture<Void> cfEmit = prod.emit("someventtype", event);
        cfEmit.get();
        latch.await();
    }
}

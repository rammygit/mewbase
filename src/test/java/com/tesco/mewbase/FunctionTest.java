package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.impl.ClientImpl;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class FunctionTest {

    private final static Logger log = LoggerFactory.getLogger(FunctionTest.class);

    private static final String TEST_STREAM1 = "com.tesco.basket";

    private Server server;
    private Client client;

    @Before
    public void before() throws Exception {
        log.trace("in before");
        server = new ServerImpl(new ServerOptions());
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();
        client = new ClientImpl();
    }

    @After
    public void after() throws Exception {
        log.trace("in after");
        client.close().get();
        server.stop().get();
    }

    @Test
    public void testSimpleFunction() {
        SubDescriptor descriptor = new SubDescriptor().setStreamName(TEST_STREAM1);
        server.installFunction("testfunc", descriptor, (ctx, re) -> {
            BsonObject event = re.event();
            long basketID = event.getInteger("basketID");
            String productID = event.getString("productID");
            int quant = event.getInteger("quantity");
            String quantPath = "items." + productID + ".quantity";

            CompletableFuture<Void> cf =
                    ctx.upsert("baskets", new BsonObject().put("id", basketID).put("$inc", new BsonObject().put(quantPath, quant)));
            cf.thenRun(() -> {
                // updated ok
            });
        });
    }
}

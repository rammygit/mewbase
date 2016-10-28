package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.Connection;
import com.tesco.mewbase.client.ConnectionOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.client.impl.ClientImpl;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created by Jamie on 14/10/2016.
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest extends ServerTestBase {
    private final static Logger log = LoggerFactory.getLogger(QueryTest.class);

    private static final String TEST_BINDER1 = "baskets";

    @Test
    public void testGetById(TestContext context) throws Exception {
        String docId = "testdoc1";

        insertDocument(TEST_BINDER1, new BsonObject().put("id", docId).put("foo", "bar")).get();

        Connection conn = client.connect(new ConnectionOptions()).get();
        BsonObject doc = conn.getByID(TEST_BINDER1, docId).get();

        Assert.assertEquals(docId, doc.getString("id"));
        Assert.assertEquals("bar", doc.getString("foo"));
    }

    @Test
    public void testGetByIdReturnsNullForNonExistentDocument(TestContext context) throws Exception {
        Connection conn = client.connect(new ConnectionOptions()).get();
        BsonObject doc = conn.getByID(TEST_BINDER1, "non-existent-document").get();

        Assert.assertEquals(null, doc);
    }

    private CompletableFuture<Void> insertDocument(String binder, BsonObject document) throws Exception {
        CompletableFuture<Void> cf = new CompletableFuture<>();

        SubDescriptor descriptor = new SubDescriptor().setChannel(TEST_CHANNEL_1);
        server.installFunction("inserterfunc", descriptor, (ctx, re) -> {
            CompletableFuture<Void> cfUpsert = ctx.upsert(binder, document);
            cfUpsert.thenRun(() -> {
                server.deleteFunction("inserterfunc");
                cf.complete(null);
            });
        });

        Connection conn = client.connect(new ConnectionOptions()).get();
        Producer prod = conn.createProducer(TEST_CHANNEL_1);

        prod.emit(new BsonObject().put("foo", "bar")).get();

        return cf;
    }
}

package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.common.SubDescriptor;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Jamie on 14/10/2016.
 */
@RunWith(VertxUnitRunner.class)
public class QueryTest extends ServerTestBase {
    private final static Logger log = LoggerFactory.getLogger(QueryTest.class);

    private static final String TEST_BINDER1 = "baskets";

    @Test
    //@Repeat(value = 1000)
    public void testGetById(TestContext context) throws Exception {
        String docID = "testdoc1";

        insertDocument(TEST_BINDER1, docID, new BsonObject().put("id", docID).put("foo", "bar"));

        log.trace("Getting doc");

        BsonObject doc =
                waitForNonNull(() -> {
                    try {
                        return client.findByID(TEST_BINDER1, docID).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        Assert.assertEquals(docID, doc.getString("id"));
        Assert.assertEquals("bar", doc.getString("foo"));
    }

    @Test
    public void testGetByIdReturnsNullForNonExistentDocument(TestContext context) throws Exception {
        BsonObject doc = client.findByID(TEST_BINDER1, "non-existent-document").get();

        Assert.assertEquals(null, doc);
    }

    private void insertDocument(String binder, String docID, BsonObject document) throws Exception {

        SubDescriptor descriptor = new SubDescriptor().setChannel(TEST_CHANNEL_1);

        server.installFunction("testfunc", TEST_CHANNEL_1, ev -> true, binder, ev -> ev.getString("docID"), (basket, del) -> document);

        Producer prod = client.createProducer(TEST_CHANNEL_1);

        prod.publish(new BsonObject().put("docID", docID).put("foo", "bar")).get();
    }
}

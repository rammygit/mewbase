package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.bson.BsonPath;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class FunctionTest extends ServerTestBase {

    private final static Logger log = LoggerFactory.getLogger(FunctionTest.class);

    @Test
    public void testSimpleFunction(TestContext context) throws Exception {
        SubDescriptor descriptor =
                new SubDescriptor().setChannel(TEST_CHANNEL_1).setMatcher(new BsonObject().put("eventType", "add_item"));

        String binderName = "baskets";

        server.installFunction("testfunc", descriptor, binderName, "basketID", (basket, del) ->
            BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID"))
        );

        Producer prod = client.createProducer(TEST_CHANNEL_1);

        String basketID = "basket1234";

        prod.publish(new BsonObject().put("basketID", basketID).put("productID", "prod1").put("quantity", 10)).get();

        BsonObject basket =
                waitForNonNull(() -> {
                    try {
                        return client.findByID(binderName, basketID).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        assertEquals(basketID, basket.getString(DocManager.ID_FIELD));
        assertEquals(10, (int)basket.getBsonObject("products").getInteger("prod1"));
    }

}

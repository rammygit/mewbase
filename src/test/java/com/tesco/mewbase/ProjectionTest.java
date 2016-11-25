package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.bson.BsonPath;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.projection.impl.ProjectionManagerImpl;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.ServerImpl;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class ProjectionTest extends ServerTestBase {

    private final static Logger log = LoggerFactory.getLogger(ProjectionTest.class);

    private static final String TEST_PROJECTION_NAME1 = "testproj";

    @Test
    public void testSimpleProjection() throws Exception {

        registerProjection();

        Producer prod = client.createProducer(TEST_CHANNEL_1);

        String basketID = "basket1234";

        prod.publish(new BsonObject().put("basketID", basketID).put("productID", "prod1").put("quantity", 10)).get();

        BsonObject basket =
                waitForNonNull(() -> {
                    try {
                        return client.findByID(TEST_BINDER1, basketID).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        assertEquals(basketID, basket.getString(DocManager.ID_FIELD));
        assertEquals(10, (int)basket.getBsonObject("products").getInteger("prod1"));
    }

    private void registerProjection() {
        server.registerProjection(TEST_PROJECTION_NAME1, TEST_CHANNEL_1, ev -> true, TEST_BINDER1, ev -> ev.getString("basketID"),
                (basket, del) ->
                        BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID"))
        );
    }

    @Test
    public void testProjectionRestart() throws Exception {
        testProjectionRestart(false);
    }

    @Test
    public void testProjectionRestartWithDuplicates() throws Exception {
        testProjectionRestart(true);
    }

    private void testProjectionRestart(boolean duplicates) throws Exception {
        registerProjection();

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        String basketID = "basket1234";

        for (int i = 0; i < 10; i++) {
            prod.publish(new BsonObject().put("basketID", basketID).put("productID", "prod1").put("quantity", 1)).get();
        }

        waitUntil(() -> {
            try {
                BsonObject basket = client.findByID(TEST_BINDER1, basketID).get();
                if (basket != null && basket.getBsonObject("products").getInteger("prod1") == 10) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Projection has processed all the events, now restart
        client.close();
        server.stop().get();

        ServerOptions serverOptions = createServerOptions(logDir);
        ClientOptions clientOptions = createClientOptions();

        server = Server.newServer(serverOptions);
        server.start().get();

        client = Client.newClient(vertx, clientOptions);

        if (duplicates) {
            // We reset the durable seq last acked so we get redeliveries - the duplicate detection should
            // ignore them
            BsonObject lastSeqs = client.findByID(ServerImpl.DURABLE_SUBS_BINDER_NAME, TEST_PROJECTION_NAME1).get();
            log.trace("lastSeqs are: " + lastSeqs);
            lastSeqs.put("lastAcked", 0);
            ((ServerImpl)server).docManager().put(ServerImpl.DURABLE_SUBS_BINDER_NAME, TEST_PROJECTION_NAME1, lastSeqs).get();
        }

        registerProjection();

        // Wait a bit
        Thread.sleep(500);

        BsonObject basket = client.findByID(TEST_BINDER1, basketID).get();
        assertEquals(10, (int)basket.getBsonObject("products").getInteger("prod1"));
    }

}

package com.tesco.mewbase.auth;

import com.tesco.mewbase.ServerTestBase;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.ClientDelivery;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.common.SubDescriptor;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.Consumer;

@RunWith(VertxUnitRunner.class)
public class AuthenticationTest extends ServerTestBase {

    @Test
    public void testAuthentication(TestContext context) throws Exception {
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = context.async();

        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> async.complete();

        client.subscribe(descriptor, handler).get();

        prod.publish(sent).get();
    }
}

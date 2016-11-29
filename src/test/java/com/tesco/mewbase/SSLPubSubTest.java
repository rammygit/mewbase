package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.ClientDelivery;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.function.Consumer;

@RunWith(VertxUnitRunner.class)
public class SSLPubSubTest extends ServerTestBase {

    private final static String CERT_PATH = "src/test/resources/server-cert.pem";
    private final static String KEY_PATH = "src/test/resources/server-key.pem";

    @Override
    protected ServerOptions createServerOptions(File logDir) {
        FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions().setLogDir(logDir.getPath());

        ServerOptions serverOptions = new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setFileLogManagerOptions(fileLogManagerOptions);

        serverOptions.getNetServerOptions().setSsl(true).setPemKeyCertOptions(
                new PemKeyCertOptions()
                        .setKeyPath(KEY_PATH)
                        .setCertPath(CERT_PATH)
        );

        return serverOptions;
    }

    @Override
    protected ClientOptions createClientOptions() {
        NetClientOptions netClientOptions = new NetClientOptions()
                .setSsl(true)
                .setPemTrustOptions(
                        new PemTrustOptions().addCertPath(CERT_PATH)
                );
        return new ClientOptions().setNetClientOptions(netClientOptions);
    }

    @Test
    public void testSimplePubSub(TestContext context) throws Exception {
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = context.async();
        long now = System.currentTimeMillis();
        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            context.assertEquals(0l, re.channelPos());
            context.assertTrue(re.timeStamp() >= now);
            BsonObject event = re.event();
            context.assertEquals(sent, event);
            async.complete();
        };

        client.subscribe(descriptor, handler).get();

        prod.publish(sent).get();
    }
}

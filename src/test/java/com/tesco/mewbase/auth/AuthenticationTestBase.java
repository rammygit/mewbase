package com.tesco.mewbase.auth;

import com.tesco.mewbase.MewbaseTestBase;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientDelivery;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.client.Producer;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class AuthenticationTestBase extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(AuthenticationTestBase.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected Server server;
    protected Client client;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        log.trace("in before");
        File logDir = testFolder.newFolder();
        log.trace("Log dir is {}", logDir);

        ServerOptions serverOptions = createServerOptions(logDir);

        server = Server.newServer(serverOptions);
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();
    }

    @Override
    protected void tearDown(TestContext context) throws Exception {
        log.trace("in after");
        client.close().get();
        log.trace("client closed");
        server.stop().get();
        log.trace("server closed");
        super.tearDown(context);
    }

    protected ServerOptions createServerOptions(File logDir) throws Exception {
        FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions().setLogDir(logDir.getPath());
        return new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setFileLogManagerOptions(fileLogManagerOptions).setBinders(new String[]{TEST_BINDER1})
                .setDocsDir(testFolder.newFolder().getPath())
                .setAuthProvider(createAuthProvider());
    }

    protected ClientOptions createClientOptions(BsonObject authInfo) {
        return new ClientOptions().setNetClientOptions(new NetClientOptions()).setAuthInfo(authInfo);
    }

    protected MewbaseAuthProvider createAuthProvider() {
        return new TestAuthProvider();
    }

    protected void setupAuthClient(BsonObject authInfo) {
        ClientOptions clientOptions = createClientOptions(authInfo);
        client = Client.newClient(vertx, clientOptions);
    }

    protected void execSimplePubSub(TestContext context, BsonObject authInfo) throws Exception {
        setupAuthClient(authInfo);
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

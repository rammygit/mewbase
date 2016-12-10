package com.tesco.mewbase.auth;

import com.tesco.mewbase.MewbaseTestBase;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.*;
import com.tesco.mewbase.common.SubDescriptor;
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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthenticationTestBase extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(AuthenticationTestBase.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected Server server;
    protected Client client;

    protected File logDir;
    protected File docsDir;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        logDir = testFolder.newFolder();
        docsDir = testFolder.newFolder();
        ServerOptions serverOptions = createServerOptions(logDir);
        server = Server.newServer(vertx, serverOptions);
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();
    }

    @Override
    protected void tearDown(TestContext context) throws Exception {
        client.close().get();
        server.stop().get();
        super.tearDown(context);
    }

    protected ServerOptions createServerOptions(File logDir) throws Exception {
        return new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setBinders(new String[]{TEST_BINDER1})
                .setLogDir(logDir.getPath())
                .setDocsDir(docsDir.getPath())
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

    protected Async execSimplePubSub(boolean success, TestContext context, BsonObject authInfo) throws Exception {
        setupAuthClient(authInfo);
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = success ? context.async() : null;

        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> async.complete();

        try {
            client.subscribe(descriptor, handler).get();
            if (!success) {
                context.fail("Should throw exception");
            }
            prod.publish(sent).get();
        } catch (ExecutionException e) {
            if (!success) {
                Throwable cause = e.getCause();
                assertTrue(cause instanceof MewException);
                MewException mcause = (MewException)cause;
                assertEquals("Authentication failed", mcause.getMessage());
                assertEquals(Client.ERR_AUTHENTICATION_FAILED, mcause.getErrorCode());
            } else {
                context.fail("Exception received");
            }
        }

        return async;
    }


}

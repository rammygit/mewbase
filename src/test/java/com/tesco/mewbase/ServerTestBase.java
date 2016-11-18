package com.tesco.mewbase;

import com.tesco.mewbase.auth.DummyAuthProvider;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.unit.TestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 28/10/16.
 */
public class ServerTestBase extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(PubSubTest.class);

    private final static String CERT_PATH = "src/test/resources/server-cert.pem";
    private final static String KEY_PATH = "src/test/resources/server-key.pem";
    private final static String USERNAME = "mewbase";
    private final static String PASSWORD = "password";

    protected Server server;
    protected Client client;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        log.trace("in before");
        File logDir = testFolder.newFolder();
        log.trace("Log dir is {}", logDir);

        ServerOptions serverOptions = createServerOptions(logDir);
        ClientOptions clientOptions = createClientOptions();

        server = Server.newServer(serverOptions);
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();

        client = Client.newClient(vertx, clientOptions);
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

    protected ServerOptions createServerOptions(File logDir) {
        FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions().setLogDir(logDir.getPath());

        return new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setFileLogManagerOptions(fileLogManagerOptions)
                .setAuthProvider(new DummyAuthProvider(USERNAME, PASSWORD));
    }

    protected ClientOptions createClientOptions() {
        BsonObject authInfo = new BsonObject().put("username", USERNAME).put("password", PASSWORD);
        return new ClientOptions().setNetClientOptions(new NetClientOptions()).setAuthInfo(authInfo);
    }
}

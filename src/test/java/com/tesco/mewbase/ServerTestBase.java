package com.tesco.mewbase;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.unit.TestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by tim on 28/10/16.
 */
public class ServerTestBase extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(PubSubTest.class);

    private final static String CERT_PATH = "src/test/resources/server-cert.pem";
    private final static String KEY_PATH = "src/test/resources/server-key.pem";
    protected Server server;
    protected Client client;
    protected File logDir;
    protected File docsDir;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        log.trace("in before");
        logDir = testFolder.newFolder();
        log.trace("Log dir is {}", logDir);

        docsDir = testFolder.newFolder();
        log.trace("Docs dir is {}", docsDir);

        ServerOptions serverOptions = createServerOptions(logDir);
        ClientOptions clientOptions = createClientOptions();

        server = Server.newServer(serverOptions);
        server.start().get();

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

    protected ServerOptions createServerOptions(File logDir) throws Exception {
        ServerOptions fileLogManagerOptions = new ServerOptions().setLogDir(logDir.getPath());
        return new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setBinders(new String[]{TEST_BINDER1})
                .setDocsDir(docsDir.getPath());
    }

    protected ClientOptions createClientOptions() {
        return new ClientOptions().setNetClientOptions(new NetClientOptions());
    }
}

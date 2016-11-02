package com.tesco.mewbase;

import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.ext.unit.TestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 28/10/16.
 */
public class ServerTestBase extends MewbaseTestBase {

    private final static Logger log = LoggerFactory.getLogger(EmitSubTest.class);

    protected Server server;
    protected Client client;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        log.trace("in before");
        File logDir = testFolder.newFolder();
        log.trace("Log dir is {}", logDir);
        FileLogManagerOptions fileLogManagerOptions = new FileLogManagerOptions().setLogDir(logDir.getPath());
        ServerOptions options = new ServerOptions().setChannels(new String[]{TEST_CHANNEL_1, TEST_CHANNEL_2})
                .setFileLogManagerOptions(fileLogManagerOptions);
        server = Server.newServer(options);
        CompletableFuture<Void> cfStart = server.start();
        cfStart.get();
        client = Client.newClient(vertx, new ClientOptions());
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
}

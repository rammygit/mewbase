package com.tesco.mewbase.server.impl.transport.net;

import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.impl.Transport;
import com.tesco.mewbase.server.impl.TransportConnection;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * TCP transport
 * <p>
 * Created by tim on 21/11/16.
 */
public class NetTransport implements Transport {

    private final static Logger log = LoggerFactory.getLogger(NetTransport.class);

    private final Vertx vertx;
    private final ServerOptions serverOptions;
    private final Set<NetServer> netServers = new ConcurrentHashSet<>();
    private Consumer<TransportConnection> connectHandler;

    public NetTransport(Vertx vertx, ServerOptions options) {
        this.vertx = vertx;
        this.serverOptions = options;
    }

    public CompletableFuture<Void> start() {
        int numServers = Runtime.getRuntime().availableProcessors(); // TODO make this configurable
        log.trace("Starting " + numServers + " net servers");
        CompletableFuture[] all = new CompletableFuture[numServers];
        for (int i = 0; i < numServers; i++) {
            NetServer netServer = vertx.createNetServer(serverOptions.getNetServerOptions());
            netServer.connectHandler(this::connectHandler);
            CompletableFuture<Void> cf = new CompletableFuture<>();
            netServer.listen(serverOptions.getNetServerOptions().getPort(),
                    serverOptions.getNetServerOptions().getHost(), ar -> {
                        if (ar.succeeded()) {
                            cf.complete(null);
                        } else {
                            cf.completeExceptionally(ar.cause());
                        }
                    });
            netServers.add(netServer);
            all[i] = cf;
        }
        return CompletableFuture.allOf(all);
    }

    public CompletableFuture<Void> stop() {
        CompletableFuture[] all = new CompletableFuture[netServers.size()];
        int i = 0;
        for (NetServer server : netServers) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            server.close(ar -> {
                if (ar.succeeded()) {
                    cf.complete(null);
                } else {
                    cf.completeExceptionally(ar.cause());
                }
            });
            all[i++] = cf;
        }
        netServers.clear();
        return CompletableFuture.allOf(all);
    }

    @Override
    public void connectHandler(Consumer<TransportConnection> connectionHandler) {
        this.connectHandler = connectionHandler;
    }

    private void connectHandler(NetSocket socket) {
        connectHandler.accept(new NetTransportConnection(socket));
    }
}

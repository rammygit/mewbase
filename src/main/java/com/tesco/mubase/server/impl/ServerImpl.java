package com.tesco.mubase.server.impl;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.common.FrameHandler;
import com.tesco.mubase.common.ReceivedEvent;
import com.tesco.mubase.common.FunctionContext;
import com.tesco.mubase.common.SubDescriptor;
import com.tesco.mubase.server.Server;
import com.tesco.mubase.server.ServerOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {

    private final static Logger log = LoggerFactory.getLogger(ServerImpl.class);

    private final ServerOptions serverOptions;
    private final Vertx vertx;
    private final Map<String, StreamProcessor> streamProcessors = new ConcurrentHashMap<>();
    private final Set<ServerConnectionImpl> connections = new ConcurrentHashSet<>();
    private final Set<NetServer> netServers = new ConcurrentHashSet<>();

    public ServerImpl(Vertx vertx, ServerOptions serverOptions) {
        this.vertx = vertx;
        this.serverOptions = serverOptions;
    }

    public ServerImpl(ServerOptions serverOptions) {
        this(Vertx.vertx(), serverOptions);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        int procs = Runtime.getRuntime().availableProcessors(); // TODO make this configurable
        procs = 1;
        log.trace("Starting " + procs + " instances");
        CompletableFuture[] all = new CompletableFuture[procs];
        for (int i = 0; i < procs; i++) {
            NetServer netServer = vertx.createNetServer();
            netServer.connectHandler(this::connectHandler);
            CompletableFuture<Void> cf = new CompletableFuture<>();
            netServer.listen(serverOptions.getPort(), serverOptions.getHost(), ar -> {
                if (ar.succeeded()) {
                    cf.complete(null);
                } else {
                    cf.completeExceptionally(ar.cause());
                }
            });
            netServers.add(netServer);
            all[i] = cf;
        }
        CompletableFuture<Void> tot = CompletableFuture.allOf(all);
        return tot;
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        CompletableFuture[] all = new CompletableFuture[netServers.size()];
        int i = 0;
        for (NetServer server: netServers) {
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
        streamProcessors.clear();
        connections.clear();
        netServers.clear();
        CompletableFuture<Void> tot = CompletableFuture.allOf(all);
        return tot;
    }

    @Override
    public void installFunction(String functionName, SubDescriptor descriptor, BiConsumer<FunctionContext, ReceivedEvent> function) {

    }

    @Override
    public void uninstallFunction(String functionName) {

    }

    @Override
    public void createBinder(String binderName) {

    }

    private void connectHandler(NetSocket socket) {
        ServerConnectionImpl conn = new ServerConnectionImpl(this, socket, Vertx.currentContext());
        connections.add(conn);
    }

    protected StreamProcessor getStreamProcessor(String streamName) {
        StreamProcessor processor = streamProcessors.get(streamName);
        if (processor == null) {
            processor = new StreamProcessor(streamName);
            StreamProcessor old = streamProcessors.putIfAbsent(streamName, processor);
            if (old != null) {
                processor = old;
            }
        }
        return processor;
    }

    protected void removeConnection(ServerConnectionImpl connection) {
        connections.remove(connection);
    }

}

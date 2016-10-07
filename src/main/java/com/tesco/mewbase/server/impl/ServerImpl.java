package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.common.ReceivedEvent;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.impl.inmem.InMemoryDocManager;
import com.tesco.mewbase.function.FunctionContext;
import com.tesco.mewbase.function.FunctionManager;
import com.tesco.mewbase.function.impl.FunctionManagerImpl;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import com.tesco.mewbase.log.impl.inmem.InMemoryLogManager;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final LogManager logManager;
    private final DocManager docManager;
    private final FunctionManager functionManager;

    public ServerImpl(Vertx vertx, ServerOptions serverOptions) {
        this.vertx = vertx;
        this.serverOptions = serverOptions;
        this.logManager = new InMemoryLogManager();
        this.docManager = new InMemoryDocManager();
        this.functionManager = new FunctionManagerImpl(docManager);
    }

    public ServerImpl(ServerOptions serverOptions) {
        this(Vertx.vertx(), serverOptions);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        int procs = Runtime.getRuntime().availableProcessors(); // TODO make this configurable
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
        return CompletableFuture.allOf(all);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
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
        streamProcessors.clear();
        connections.clear();
        netServers.clear();
        return CompletableFuture.allOf(all);
    }

    @Override
    public boolean installFunction(String functionName, SubDescriptor descriptor,
                                   BiConsumer<FunctionContext, ReceivedEvent> function) {
        return functionManager.installFunction(functionName, descriptor, function);
    }

    @Override
    public boolean deleteFunction(String functionName) {
        return functionManager.deleteFunction(functionName);
    }

    private void connectHandler(NetSocket socket) {
        ServerConnectionImpl conn = new ServerConnectionImpl(this, socket, Vertx.currentContext());
        connections.add(conn);
    }

    protected StreamProcessor getStreamProcessor(String streamName) {
        StreamProcessor processor = streamProcessors.get(streamName);
        if (processor == null) {
            processor = new StreamProcessor(streamName, getLog(streamName));
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

    protected Log getLog(String streamName) {
        return logManager.getLog(streamName);
    }

}

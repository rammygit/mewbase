package com.tesco.mewbase.server.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.impl.inmem.InMemoryDocManager;
import com.tesco.mewbase.function.FunctionManager;
import com.tesco.mewbase.function.impl.FunctionManagerImpl;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import com.tesco.mewbase.log.impl.file.FileAccess;
import com.tesco.mewbase.log.impl.file.FileLogManager;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.log.impl.file.faf.AFFileAccess;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 22/09/16.
 */
public class ServerImpl implements Server {

    private final static Logger log = LoggerFactory.getLogger(ServerImpl.class);

    private final ServerOptions serverOptions;
    private final Vertx vertx;
    private final Set<ConnectionImpl> connections = new ConcurrentHashSet<>();
    private final Set<NetServer> netServers = new ConcurrentHashSet<>();
    private final LogManager logManager;
    private final DocManager docManager;
    private final FunctionManager functionManager;

    protected ServerImpl(Vertx vertx, ServerOptions serverOptions) {
        this.vertx = vertx;
        this.serverOptions = serverOptions;
        FileAccess faf = new AFFileAccess(vertx);
        FileLogManagerOptions sOptions = serverOptions.getFileLogManagerOptions();
        FileLogManagerOptions options = sOptions == null ? new FileLogManagerOptions() : sOptions;
        this.logManager = new FileLogManager(vertx, options, faf);
        this.docManager = new InMemoryDocManager();
        this.functionManager = new FunctionManagerImpl(docManager, logManager);
    }

    protected ServerImpl(ServerOptions serverOptions) {
        this(Vertx.vertx(), serverOptions);
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
        int procs = Runtime.getRuntime().availableProcessors(); // TODO make this configurable
        log.trace("Starting " + procs + " instances");
        String[] channels = serverOptions.getChannels();
        CompletableFuture[] all = new CompletableFuture[procs + (channels != null ? channels.length : 0)];

        for (int i = 0; i < procs; i++) {
            NetServer netServer = vertx.createNetServer(serverOptions.getNetServerOptions());
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
        // Start the channels
        if (serverOptions.getChannels() != null) {
            for (String channel : serverOptions.getChannels()) {
                all[procs++] = logManager.createLog(channel);
            }
        }
        return CompletableFuture.allOf(all);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() {
        CompletableFuture[] all = new CompletableFuture[netServers.size() + 1];
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
        all[all.length - 1] = logManager.close();
        connections.clear();
        netServers.clear();
        return CompletableFuture.allOf(all);
    }


    @Override
    public boolean installFunction(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                           String binderName, Function<BsonObject, String> docIDSelector,
                           BiFunction<BsonObject, Delivery, BsonObject> function) {

        return functionManager.installFunction(name, channel, eventFilter, binderName, docIDSelector, function);
    }

    @Override
    public boolean deleteFunction(String functionName) {
        return functionManager.deleteFunction(functionName);
    }

    private void connectHandler(NetSocket socket) {
        ConnectionImpl conn = new ConnectionImpl(this, socket, Vertx.currentContext(), docManager);
        connections.add(conn);
    }

    protected void removeConnection(ConnectionImpl connection) {
        connections.remove(connection);
    }

    protected Log getLog(String channel) {
        return logManager.getLog(channel);
    }

}

package com.tesco.mewbase.server;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.Delivery;
import com.tesco.mewbase.projection.Projection;
import com.tesco.mewbase.projection.ProjectionBuilder;
import com.tesco.mewbase.projection.ProjectionManager;
import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 21/09/16.
 */
public interface Server {

    static Server newServer(ServerOptions serverOptions) {
        return factory.newServer(serverOptions);
    }

    Projection registerProjection(String name, String channel, Function<BsonObject, Boolean> eventFilter,
                                  String binderName, Function<BsonObject, String> docIDSelector,
                                  BiFunction<BsonObject, Delivery, BsonObject> projectionFunction);

    ProjectionBuilder buildProjection(String name);

    CompletableFuture<Void> start();

    CompletableFuture<Void> stop();

    ServerFactory factory = ServiceHelper.loadFactory(ServerFactory.class);

}

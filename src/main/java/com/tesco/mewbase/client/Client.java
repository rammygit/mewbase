package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.spi.ClientFactory;
import com.tesco.mewbase.common.DocQuerier;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;
import com.tesco.mewbase.server.spi.ServerFactory;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 21/09/16.
 */
public interface Client extends DocQuerier {

    static Client newClient(ClientOptions options) {
        return factory.newClient(options);
    }

    static Client newClient(Vertx vertx, ClientOptions options) {
        return factory.newClient(vertx, options);
    }

    ClientFactory factory = ServiceHelper.loadFactory(ClientFactory.class);


    CompletableFuture<Subscription> subscribe(SubDescriptor subDescriptor);

    Producer createProducer(String channel);

    CompletableFuture<Void> emit(String channel, BsonObject event);

    CompletableFuture<Void> emit(String channel, BsonObject event, Function<BsonObject, String> partitionFunc);

    CompletableFuture<Void> close();

}

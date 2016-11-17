package com.tesco.mewbase.client;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.spi.ClientFactory;
import com.tesco.mewbase.common.SubDescriptor;
import io.vertx.core.ServiceHelper;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by tim on 21/09/16.
 */
public interface Client {

    static Client newClient(ClientOptions options) {
        return factory.newClient(options);
    }

    static Client newClient(Vertx vertx, ClientOptions options) {
        return factory.newClient(vertx, options);
    }

    ClientFactory factory = ServiceHelper.loadFactory(ClientFactory.class);

    CompletableFuture<BsonObject> findByID(String binderName, String id);

    void findMatching(String binderName, BsonObject matcher,
                      Consumer<QueryResult> resultHandler, Consumer<Throwable> exceptionHandler);

    CompletableFuture<Subscription> subscribe(SubDescriptor subDescriptor, Consumer<ClientDelivery> handler);

    Producer createProducer(String channel);

    CompletableFuture<Void> publish(String channel, BsonObject event);

    CompletableFuture<Void> publish(String channel, BsonObject event, Function<BsonObject, String> partitionFunc);

    CompletableFuture<Void> close();

}

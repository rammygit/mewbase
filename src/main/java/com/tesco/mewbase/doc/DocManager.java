package com.tesco.mewbase.doc;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.log.LogReadStream;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface DocManager {

    String ID_FIELD = "id";

    DocReadStream getMatching(String binderName, Function<BsonObject, Boolean> matcher);

    CompletableFuture<BsonObject> get(String binderName, String id);

    CompletableFuture<Void> put(String binder, String id, BsonObject doc);

    CompletableFuture<Boolean> delete(String binder, String id);

    CompletableFuture<Void> close();

    CompletableFuture<Void> start();

    CompletableFuture<Void> createBinder(String binderName);
}

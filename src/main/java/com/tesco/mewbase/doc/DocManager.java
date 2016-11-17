package com.tesco.mewbase.doc;

import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 *
 * Created by tim on 30/09/16.
 */
public interface DocManager {

    //TODO: should probable be public static final
    String ID_FIELD = "id";

    DocReadStream getMatching(String binderName, Function<BsonObject, Boolean> matcher);

    CompletableFuture<BsonObject> get(String binderName, String id);

    /**
     * Save a named document to a binder with the given name
     * @param binder name of binder to save the document to
     * @param id the name of the document within the binder
     * @param doc the document to save
     * @return
     */
    CompletableFuture<Void> put(String binder, String id, BsonObject doc);

    CompletableFuture<Boolean> delete(String binder, String id);

    CompletableFuture<Void> close();

    CompletableFuture<Void> start();

    CompletableFuture<Void> createBinder(String binderName);
    
}

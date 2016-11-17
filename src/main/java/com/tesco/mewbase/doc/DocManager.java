package com.tesco.mewbase.doc;

import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public interface DocManager {

    //TODO: should probably be public static final
    String ID_FIELD = "id";

    DocReadStream getMatching(String binder, Function<BsonObject, Boolean> matcher);

    /**
     * Get a document in a named binder with the given id
     *
     * @param binder name of binder to put the document to
     * @param id     the name of the document within the binder
     * @return
     */

    CompletableFuture<BsonObject> get(String binder, String id);

    /**
     * Put a document in a named binder at the given id
     *
     * @param binder name of binder to put the document to
     * @param id     the name of the document within the binder
     * @param doc    the document to save
     * @return
     */
    CompletableFuture<Void> put(String binder, String id, BsonObject doc);

    /**
     * Delete a document from a binder identified with the given id
     *
     * @param binder name of binder to put the document to
     * @param id     the name of the document within the binder
     * @return a CompleteableFuture with a Boolean set to true if successful
     */
    CompletableFuture<Boolean> delete(String binder, String id);

    CompletableFuture<Void> close();

    CompletableFuture<Void> start();

    CompletableFuture<Void> createBinder(String binderName);
}

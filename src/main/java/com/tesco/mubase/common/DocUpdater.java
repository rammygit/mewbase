package com.tesco.mubase.common;

import com.tesco.mubase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 23/09/16.
 */
public interface DocUpdater extends Transactional {

    /**
     * Inserts a new document with the specified id
     * @param binderName
     * @param id
     * @param doc
     * @return
     */
    CompletableFuture<Void> insertDocument(String binderName, String id, BsonObject doc);

    CompletableFuture<Void> deleteDocument(String binderName, String id);

    /**
     * Updates document with specified id
     * @param binderName
     * @param id
     * @param updateSpec - determines which top level fields to update or uses dot notations find nested fields/arrays
     * @return
     */
    CompletableFuture<Void> updateDocument(String binderName, String id, BsonObject updateSpec);

    /**
     * Updates documents in binder with specified id
     * @param binderName
     * @param updateSpec - determines which top level fields to update or uses dot notations find nested fields/arrays
     * @return
     */
    CompletableFuture<Void> updateDocuments(String binderName, BsonObject updateSpec);

}

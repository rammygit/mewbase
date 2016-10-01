package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 23/09/16.
 */
public interface DocUpdater extends Transactional {

    CompletableFuture<Void> insert(String binderName, BsonObject doc);

    CompletableFuture<Void> upsert(String binderName, BsonObject doc);

    CompletableFuture<Void> createLink(String sourceBinderName, String sourceID, String targetBinderName, String targetID);

    CompletableFuture<Void> deleteLink(String sourceBinderName, String sourceID, String targetBinderName, String targetID);

    CompletableFuture<Void> delete(String binderName, String id);

    CompletableFuture<Void> deleteMatching(String binderName, BsonObject matcher);

    /**
     * Updates document with specified id
     * @param binderName
     * @param id
     * @param updateSpec - determines which top level fields to update or uses dot notations find nested fields/arrays
     * @return
     */
    CompletableFuture<Void> updateByID(String binderName, String id, BsonObject updateSpec);

    /**
     * Updates documents in binder with specified id
     * @param binderName
     * @param updateSpec - determines which top level fields to update or uses dot notations find nested fields/arrays
     * @return
     */
    CompletableFuture<Void> updateMatching(String binderName, BsonObject matcher, BsonObject updateSpec);

}

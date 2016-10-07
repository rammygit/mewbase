package com.tesco.mewbase.function.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.QueryResult;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.function.FunctionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 01/10/16.
 */
public class FunctionContextImpl implements FunctionContext {

    private final DocManager docManager;

    public FunctionContextImpl(DocManager docManager) {
        this.docManager = docManager;
    }

    @Override
    public boolean startTx() {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> commitTx() {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> abortTx() {
        return null;
    }

    @Override
    public CompletableFuture<Void> insert(String binderName, BsonObject doc) {
        return null;
    }

    @Override
    public CompletableFuture<Void> upsert(String binderName, BsonObject doc) {
        return docManager.upsert(binderName, doc);
    }

    @Override
    public CompletableFuture<Void> createLink(String sourceBinderName, String sourceID, String targetBinderName, String targetID) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteLink(String sourceBinderName, String sourceID, String targetBinderName, String targetID) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String binderName, String id) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteMatching(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateByID(String binderName, String id, BsonObject updateSpec) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateMatching(String binderName, BsonObject matcher, BsonObject updateSpec) {
        return null;
    }

    @Override
    public CompletableFuture<BsonObject> getByID(String binderName, String id) {
        return null;
    }

    @Override
    public CompletableFuture<BsonObject> getByMatch(String binderName, String id) {
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> getAllMatching(String binderName, BsonObject matcher) {
        return null;
    }
}

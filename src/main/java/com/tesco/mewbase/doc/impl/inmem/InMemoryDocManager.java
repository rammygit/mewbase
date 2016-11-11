package com.tesco.mewbase.doc.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.QueryResultHandler;
import com.tesco.mewbase.client.QueryResult;
import com.tesco.mewbase.doc.DocManager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by tim on 30/09/16.
 */
public class InMemoryDocManager implements DocManager {

    private final Map<String, Binder> binders = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<BsonObject> findByID(String binderName, String id) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<BsonObject> cfResult = new CompletableFuture<>();
        BsonObject doc = binder.getDocument(id);
        cfResult.complete(doc);
        return cfResult;
    }

    @Override
    public CompletableFuture<Void> save(String binderName, String id, BsonObject doc) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<Void> cfResult = new CompletableFuture<>();
        binder.save(id, doc);
        cfResult.complete(null);
        return cfResult;
    }

    @Override
    public CompletableFuture<QueryResultHandler> findMatching(String binderName, BsonObject matcher, Consumer<QueryResult> resultHandler) {
        return null;
    }

    private Binder getOrCreateBinder(String binderName) {
        Binder binder = binders.get(binderName);
        if (binder == null) {
            binder = new Binder();
            Binder prev = binders.putIfAbsent(binderName, binder);
            if (prev != null) {
                binder = prev;
            }
        }
        return binder;
    }

}

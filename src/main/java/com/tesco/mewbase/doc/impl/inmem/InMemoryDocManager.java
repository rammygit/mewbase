package com.tesco.mewbase.doc.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.doc.DocReadStream;
import com.tesco.mewbase.doc.DocManager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Created by tim on 30/09/16.
 */
public class InMemoryDocManager implements DocManager {

    private final Map<String, Binder> binders = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<BsonObject> get(String binderName, String id) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<BsonObject> cfResult = new CompletableFuture<>();
        BsonObject doc = binder.getDocument(id);
        cfResult.complete(doc);
        return cfResult;
    }

    @Override
    public CompletableFuture<Void> put(String binderName, String id, BsonObject doc) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<Void> cfResult = new CompletableFuture<>();
        binder.save(id, doc);
        cfResult.complete(null);
        return cfResult;
    }

    @Override
    public CompletableFuture<Boolean> delete(String binderName, String id) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<Boolean> cfResult = new CompletableFuture<>();
        cfResult.complete(binder.delete(id));
        return cfResult;
    }

    @Override
    public DocReadStream getMatching(String binderName, Function<BsonObject, Boolean> matcher) {
        return null;
    }

    @Override
    public CompletableFuture<Void> createBinder(String binderName) {
        return null;
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> start() {
        return CompletableFuture.completedFuture(null);
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

package com.tesco.mewbase.doc.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MuException;
import com.tesco.mewbase.client.QueryResult;
import com.tesco.mewbase.doc.DocManager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 30/09/16.
 */
public class InMemoryDocManager implements DocManager {

    private final Map<String, Binder> binders = new ConcurrentHashMap<>();

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
    public CompletableFuture<BsonObject> getByID(String binderName, String id) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<BsonObject> cfResult = new CompletableFuture<>();
        BsonObject doc = binder.getDocument(id);
        if (doc == null) {
            // TODO should we return success with result null in this case?
            cfResult.complete(null);
        } else {
            cfResult.complete(doc);
        }
        return cfResult;
    }

    @Override
    public CompletableFuture<BsonObject> getByMatch(String binderName, String id) {
        return null;
    }

    @Override
    public CompletableFuture<QueryResult> getAllMatching(String binderName, BsonObject matcher) {
        return null;
    }

    @Override
    public CompletableFuture<Void> insert(String binderName, BsonObject doc) {
        return null;
    }

    @Override
    public CompletableFuture<Void> upsert(String binderName, BsonObject doc) {
        Binder binder = getOrCreateBinder(binderName);
        CompletableFuture<Void> cfResult = new CompletableFuture<>();
        String id = doc.getString(ID_FIELD);
        if (id == null) {
            // TODO - should we always provide error codes?
            cfResult.completeExceptionally(new MuException("No id field in BsonObject"));
        } else {
            binder.upsert(id, doc);
            cfResult.complete(null);
        }
        return cfResult;
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

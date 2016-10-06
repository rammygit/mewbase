package com.tesco.mewbase.storage.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.storage.StorageManager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Not a serious storage manager. Can be useful for tests
 *
 * Created by tim on 05/10/16.
 */
public class InMemStorageManager implements StorageManager {

    private final Map<String, BsonObject> map = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<BsonObject> get(String id) {
        CompletableFuture<BsonObject> cf = new CompletableFuture<>();
        cf.complete(map.get(id));
        return cf;
    }

    @Override
    public CompletableFuture<Void> put(String id, BsonObject doc) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        map.put(id, doc);
        cf.complete(null);
        return cf;
    }

    @Override
    public CompletableFuture<Boolean> remove(String id) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        cf.complete(map.remove(id) != null);
        return cf;
    }

    @Override
    public ReadStream rangeQuery(String start, String end) {
        return null;
    }
}

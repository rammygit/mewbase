package com.tesco.mewbase.storage.impl.bplustree;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.storage.StorageManager;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 05/10/16.
 */
public class BPlusTreeStorageManager implements StorageManager {
    @Override
    public CompletableFuture<BsonObject> get(String id) {
        return null;
    }

    @Override
    public CompletableFuture<Void> put(String id, BsonObject doc) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> remove(String id) {
        return null;
    }

    @Override
    public ReadStream rangeQuery(String start, String end) {
        return null;
    }
}

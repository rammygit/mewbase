package com.tesco.mewbase.storage;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 05/10/16.
 */
public interface StorageManager {

    CompletableFuture<BsonObject> get(String id);

    CompletableFuture<Void> put(String id, BsonObject doc);

    CompletableFuture<Boolean> remove(String id);

    ReadStream rangeQuery(String start, String end);

}

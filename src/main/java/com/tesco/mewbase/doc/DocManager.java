package com.tesco.mewbase.doc;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.DocQuerier;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 30/09/16.
 */
public interface DocManager extends DocQuerier {

    String ID_FIELD = "id";

    CompletableFuture<Void> save(String binder, String id, BsonObject doc);
}

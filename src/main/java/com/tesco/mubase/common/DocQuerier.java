package com.tesco.mubase.common;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.client.QueryResult;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 23/09/16.
 */
public interface DocQuerier {

    // TODO use RxJava for this too
    CompletableFuture<QueryResult> query(String binderName, BsonObject matcher);

    CompletableFuture<BsonObject> queryOne(String binderName, BsonObject matcher);

}

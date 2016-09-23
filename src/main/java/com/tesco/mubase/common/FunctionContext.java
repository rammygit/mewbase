package com.tesco.mubase.common;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public interface FunctionContext {
    CompletableFuture<BsonObject> getDocument(String binderName, String id);
}

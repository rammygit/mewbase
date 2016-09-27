package com.tesco.mewbase.log;

import com.tesco.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface Log {

    LogReadStream openReadStream(long mewbase);

    LogWriteStream openWriteStream();

    CompletableFuture<Void> append(BsonObject obj);
}

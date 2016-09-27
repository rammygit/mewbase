package com.tesco.mubase.log;

import com.tesco.mubase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface Log {

    LogReadStream openReadStream(long seqNumber);

    LogWriteStream openWriteStream();

    CompletableFuture<Void> append(BsonObject obj);
}

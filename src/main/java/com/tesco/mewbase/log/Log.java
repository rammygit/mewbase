package com.tesco.mewbase.log;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface Log {

    CompletableFuture<ReadStream> openReadStream(long pos);

    CompletableFuture<Long> append(BsonObject obj);

    CompletableFuture<Void> start();

    void close();
}

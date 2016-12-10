package com.tesco.mewbase.log;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.SubDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface Log {

    LogReadStream subscribe(SubDescriptor subDescriptor);

    CompletableFuture<Long> append(BsonObject obj);

    CompletableFuture<Void> start();

    CompletableFuture<Void> close();

    int getFileNumber();

    long getHeadPos();

    int getFilePos();
}
